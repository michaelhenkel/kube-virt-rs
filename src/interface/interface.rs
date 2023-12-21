use std::{sync::Arc, time::Duration};
use kube_runtime::reflector::ObjectRef;
use rand::Rng;
use k8s_openapi::{chrono::Local, api::core::v1::LocalObjectReference, apimachinery::pkg::apis::meta::v1::OwnerReference};
use kube::{CustomResource, runtime::{Controller, watcher::Config, controller::Action}, Api, client, core::ObjectMeta, Resource};
use serde::{Deserialize, Serialize};
use garde::Validate;
use schemars::JsonSchema;
use futures::channel::mpsc;
use tracing::{warn, info};
use futures::stream::StreamExt;

use crate::{instance::instance::{Instance, InstanceInterface}, resource::resource::{ReconcileError, ResourceClient}, network::network::{Network, self}, ipaddress::{ipaddress::{Ipaddress, IpaddressSpec}, self}, lxdmanager::lxdmanager::InterfaceConfig};

#[derive(CustomResource, Deserialize, Serialize, Clone, Debug, Validate, JsonSchema)]
#[kube(group = "virt.dev", version = "v1", kind = "Interface", namespaced)]
#[kube(status = "InterfaceStatus")]
#[kube(printcolumn = r#"{"name":"Ip", "jsonPath": ".spec.metadata.team", "type": "string"}"#)]
#[serde(rename_all = "camelCase")]
pub struct InterfaceSpec {
    #[garde(skip)]
    pub name: String,
    #[garde(skip)]
    pub mgmt: Option<bool>,
    #[garde(skip)]
    pub mtu: u32,
    #[garde(skip)]
    pub network: LocalObjectReference,
    #[garde(skip)]
    pub pci_idx: u32,
}

#[derive(Deserialize, Serialize, Clone, Debug, Default, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct InterfaceStatus {
    state: InstanceInterface,
    pub defined: bool,
}

impl Interface{
    pub fn controller(client: client::Client, update_rx: mpsc::Receiver<ObjectRef<Interface>>) -> Controller<Interface> {
        let api = Api::<Interface>::all(client.clone());
        Controller::new(api, Config::default())
        .reconcile_on(update_rx.map(|x| (x)))
        .watches(
            Api::<Instance>::all(client),
            Config::default(),
            |object| {
                let object_list = Vec::new();
                object_list.into_iter()
            }
        )
    }
    pub async fn reconcile(g: Arc<Interface>, ctx: Arc<ResourceClient<Interface>>) ->  Result<Action, ReconcileError> {
        ctx.setup_finalizer(g, ctx.clone(), Interface::apply, Interface::cleanup).await
    }

    pub async fn apply(g: Arc<Interface>, ctx: Arc<ResourceClient<Interface>>) ->  Result<Action, ReconcileError> {
        info!("reconciling Interface: {:?}", g.metadata.name);
        let mut interface = match ctx.get::<Interface>(&g.metadata).await?{
            Some(interface) => {
                interface
            },
            None => {
                warn!("interface not found, probably deleted");
                return Ok(Action::await_change());
            }
        };
        let name = interface.metadata.name.clone().unwrap();
        let namespace = interface.metadata.namespace.clone().unwrap();

        if interface.status.is_none(){
            interface.status = Some(InterfaceStatus::default());
            interface.status.as_mut().unwrap().defined = false;
        }
        let mut instance_name = None;
        if let Some(owner_refs) = &interface.meta().owner_references{
            for owner_ref in owner_refs{
                if owner_ref.kind == "Instance" {
                    instance_name = Some(owner_ref.name.clone());
                    break;
                }
            }
        }
        let instance_name = match instance_name{
            Some(instance_name) => instance_name,
            None => {
                warn!("interface has no owner");
                return Err(ReconcileError(anyhow::anyhow!("interface has no owner")));
            }
        };

        let interface_state =  match ctx.lxd_client.as_ref().unwrap().interface_status(instance_name.clone(), interface.spec.name.clone()).await{
            Ok(interface_state) => interface_state,
            Err(e) => {
                warn!("failed to get interface state: {:?}", e);
                return Err(ReconcileError(anyhow::anyhow!("failed to get interface state: {:?}", e)));
            }
        };

        if let Some(interface_state) = interface_state{
            info!("interface state found for {}", name);
            if interface.status.as_ref().unwrap().state != interface_state{
                info!("updating interface state {:?}",interface_state);
                interface.status.as_mut().unwrap().state = interface_state.clone();
                ctx.update_status(&interface).await?;
            }
        } else if !interface.status.as_ref().unwrap().defined{
            info!("interface state not found for {}, creating interface", name);
            let mac = generate_mac_address();
            let network_metadata = ObjectMeta{
                name: interface.spec.network.name.clone(),
                namespace: Some(g.metadata.namespace.clone().unwrap()),
                ..Default::default()
            };
            let mut network = match ctx.get::<network::Network>(&network_metadata).await?{
                Some(network) => {
                    network
                },
                None => {
                    warn!("network not found: {:?}", network_metadata);
                    return Err(ReconcileError(anyhow::anyhow!("network not found")));
                }
            };
            let (ip, prefix_len) = if let Some(mut network_status) = network.status{
                network_status.unused.sort();
                let ip = if let Some(mut ip_dec) = network_status.unused.pop(){
                    ip_dec += 1;
                    let ip = std::net::Ipv4Addr::from(ip_dec);
                    if network_status.unused.len() == 0{
                        network_status.last_ip = ip_dec;
                    }
                    ip.to_string()
                } else {
                    let ip_dec = network_status.last_ip + 1;
                    let ip = std::net::Ipv4Addr::from(ip_dec);
                    network_status.last_ip = ip_dec;
                    ip.to_string()
                };
                network.status = Some(network_status);
                if let Err(e) = ctx.update_status(&network).await{
                    return Err(ReconcileError(anyhow::anyhow!("failed to update network status: {:?}", e)));
                }
                let ip_net: ipnet::Ipv4Net = network.spec.subnet.parse().unwrap();
                let prefix_len = ip_net.prefix_len();
                (ip, prefix_len)
            } else {
                return Err(ReconcileError(anyhow::anyhow!("network status not found")));
            };

            let interface_config = InterfaceConfig{
                name: interface.spec.name.clone(),
                mac: mac.clone(),
                mtu: interface.spec.mtu,
                ipv4: ip,
                prefix_len,
                pci_idx: interface.spec.pci_idx,
            };
            
            if let Err(e) = ctx.lxd_client.as_ref().unwrap().define_interface(instance_name.clone(), interface_config).await{
                return Err(ReconcileError(e));
            }
            interface.status.as_mut().unwrap().defined = true;
            ctx.update_status(&interface).await?;
        }

        Ok(Action::requeue(Duration::from_secs(5 * 300)))
    }


    pub async fn cleanup(g: Arc<Interface>, ctx: Arc<ResourceClient<Interface>>) ->  Result<Action, ReconcileError> {      
        info!("cleaning up Interface: {:?}", g.metadata.name);
        if let Err(e) = ctx.lxd_client.as_ref().unwrap().delete(g.metadata.name.clone().unwrap()).await{
            warn!("failed to delete Interface: {:?}", e);
            return Err(ReconcileError(e));
        }
        return Ok(Action::requeue(std::time::Duration::from_secs(5 * 60)))
    }

    pub fn error_policy(_g: Arc<Interface>, error: &ReconcileError, _ctx: Arc<ResourceClient<Interface>>) -> Action {
        warn!("reconcile failed: {:?}", error);
        Action::requeue(Duration::from_secs(5 * 60))
    }
}

fn generate_mac_address() -> String {
    let mut rng = rand::thread_rng();
    let mut mac = String::new();
    for i in 0..6 {
        let mut number = rng.gen_range(0..255);
        if i == 0 {
            number = match unset_bit(number, 0) {
                Ok(val) => val,
                Err(e) => panic!("{}", e),
            };
        }
        if i != 0 {
            mac.push(':');
        }
        mac.push_str(&format!("{:02X}", number));
    }
    mac.to_lowercase()
}

fn unset_bit(b: u8, bit_number: i32) -> Result<u8, &'static str> {
    if bit_number < 8 && bit_number > -1 {
        Ok(b & !(0x01 << bit_number))
    } else {
        Err("BitNumber was not in the valid range! (BitNumber = (min)0 - (max)7)")
    }
}
