use std::{sync::Arc, time::Duration};
use rand::Rng;
use k8s_openapi::{chrono::Local, api::core::v1::LocalObjectReference, apimachinery::pkg::apis::meta::v1::OwnerReference};
use kube::{CustomResource, runtime::{Controller, watcher::Config, controller::Action}, Api, client, core::ObjectMeta, Resource};
use serde::{Deserialize, Serialize};
use garde::Validate;
use schemars::JsonSchema;
use tracing::{warn, info};

use crate::{instance::instance::Instance, resource::resource::{ReconcileError, ResourceClient}, network::network::Network, ipaddress::{ipaddress::{Ipaddress, IpaddressSpec}, self}};

#[derive(CustomResource, Deserialize, Serialize, Clone, Debug, Validate, JsonSchema)]
#[kube(group = "virt.dev", version = "v1", kind = "Interface", namespaced)]
#[kube(status = "InterfaceStatus")]
#[kube(printcolumn = r#"{"name":"Ip", "jsonPath": ".spec.metadata.team", "type": "string"}"#)]
#[serde(rename_all = "camelCase")]
pub struct InterfaceSpec {
    #[schemars(length(min = 3))]
    #[garde(length(min = 3))]
    pub name: String,
    #[garde(skip)]
    pub mtu: i32,
    #[garde(skip)]
    pub network: LocalObjectReference,
}

#[derive(Deserialize, Serialize, Clone, Debug, Default, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct InterfaceStatus {
    ip: Option<String>,
    mac: String,
}

impl Interface{
    pub fn controller(client: client::Client) -> Controller<Interface> {
        let api = Api::<Interface>::all(client.clone());
        Controller::new(api, Config::default())
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
        let mut interface = match ctx.get::<Interface>(&g.metadata).await?{
            Some(interface) => {
                interface
            },
            None => {
                warn!("interface not found, probably deleted");
                return Ok(Action::await_change());
            }
        };
        let name = interface.metadata.name.clone();
        let namespace = interface.metadata.namespace.clone();
        if let Some(status) = &mut interface.status{
            if status.ip.is_none(){
                let metadata = ObjectMeta{
                    name: name.clone(),
                    namespace: namespace.clone(),
                    ..Default::default()
                };
                match ctx.get::<Ipaddress>(&metadata).await?{
                    Some(mut ipaddress) => {
                        if let Some(ip_status) = &mut ipaddress.status{
                            if let Some(ip) = &ip_status.ip{
                                status.ip = Some(ip.clone());
                                ctx.update_status(&interface).await?;
                            }
                        }
                    },
                    None => {
                        warn!("network not found, probably deleted");
                        return Ok(Action::await_change());
                    }
                }
            }
        } else {
            let mut status = InterfaceStatus::default();
            status.mac = generate_mac_address();
            interface.status = Some(status);
            ctx.update_status(&interface).await?;
            let metadata = ObjectMeta{
                name: interface.spec.network.name.clone(),
                namespace: Some(interface.metadata.namespace.clone().unwrap()),
                owner_references: Some(vec![
                    OwnerReference{
                        api_version: "virt.dev/v1".to_string(),
                        block_owner_deletion: Some(true),
                        controller: Some(true),
                        kind: "Interface".to_string(),
                        name: interface.metadata.name.clone().unwrap(),
                        uid: interface.metadata.uid.clone().unwrap(),
                    }
                ]),
                ..Default::default()
            };
            let ipaddress = Ipaddress{
                metadata: metadata.clone(),
                spec: IpaddressSpec{
                    network: LocalObjectReference{
                        name: interface.spec.network.name.clone(),
                    },
                },
                status: None,
            };
            match ctx.create(&ipaddress).await?{
                Some(_) => {
                    info!("ipaddress created: {:?}", metadata);
                },
                None => {
                    info!("ipaddress not created: {:?}", metadata);
                }
            }
        }
        
        Ok(Action::await_change())
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
