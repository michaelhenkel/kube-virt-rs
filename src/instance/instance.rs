use std::{sync::Arc, time::Duration, collections::HashMap, any};
use k8s_openapi::{chrono::Local, api::core::v1::LocalObjectReference};
use kube::{
    CustomResource,
    runtime::{Controller, watcher::Config, controller::Action},
    Api,
    client, core::ObjectMeta
};
use kube_runtime::reflector::ObjectRef;
use rand::Rng;
use serde::{Deserialize, Serialize};
use garde::Validate;
use schemars::JsonSchema;
use tracing::{warn, info};
use futures::channel::mpsc;
use futures::stream::StreamExt;
use crate::{resource::resource::{ReconcileError, ResourceClient}, instance, interface::interface, lxdmanager::lxdmanager};
use crate::lxdmanager::lxdmanager::InstanceConfig;
use crate::network::network;


#[derive(CustomResource, Deserialize, Serialize, Clone, Debug, Validate, JsonSchema)]
#[kube(group = "virt.dev", version = "v1", kind = "Instance", namespaced)]
#[kube(status = "InstanceStatus")]
#[kube(printcolumn = r#"{"name":"Ready", "jsonPath": ".spec.status.ready", "type": "boolean"}"#)]
#[serde(rename_all = "camelCase")]
pub struct InstanceSpec {
    #[garde(skip)]
    pub memory: String,
    #[garde(skip)]
    pub image: String,
    #[garde(skip)]
    pub vcpu: i32,
    #[garde(skip)]
    pub interfaces: Vec<Interface>,
    #[garde(skip)]
    pub routes: Option<Vec<Route>>,
}

#[derive(Deserialize, Serialize, Clone, Debug, Default, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct Route{
    pub destination: RouteDestination,
    pub next_hops: Vec<RouteNextHop>,
}

#[derive(Deserialize, Serialize, Clone, Debug, Default, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct RouteDestination{
    pub instance: String,
    pub network: String,
}

#[derive(Deserialize, Serialize, Clone, Debug, Default, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct RouteNextHop{
    pub instance: String,
    pub network: String,
}

#[derive(Deserialize, Serialize, Clone, Debug, Default, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct Interface{
    pub name: String,
    pub mgmt: Option<bool>,
    pub network: LocalObjectReference,
    pub mtu: u32,
}

#[derive(Deserialize, Serialize, Clone, Debug, Default, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct InstanceStatus {
    state: String,
    networks: Option<HashMap<String, InstanceInterface>>,
    routes: Option<Vec<RouteStatus>>,
    ready: bool,
}

#[derive(Deserialize, Serialize, Clone, Debug, Default, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct RouteStatus{
    destination: String,
    next_hops: Vec<RouteNextHopStatus>,
}

#[derive(Deserialize, Serialize, Clone, Debug, Default, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct RouteNextHopStatus{
    ip: String,
    mac: String,
}

#[derive(Deserialize, Serialize, Clone, Debug, Default, JsonSchema)]
pub struct InstanceState{
    pub cpu: Cpu,
    pub disk: Disk,
    pub memory: Memory,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub network: Option<HashMap<String, InstanceInterface>>,
    pub pid: u64,
    pub processes: i64,
    pub status: String,
    pub status_code: u64,
}

#[derive(Deserialize, Serialize, Clone, Debug, Default, JsonSchema)]
pub struct Cpu{
    pub usage: u64,
}

#[derive(Deserialize, Serialize, Clone, Debug, Default, JsonSchema)]
pub struct Disk{
    pub root: Root,
}

#[derive(Deserialize, Serialize, Clone, Debug, Default, JsonSchema)]
pub struct Root{
    pub total: u64,
    pub usage: u64,
}

#[derive(Deserialize, Serialize, Clone, Debug, Default, JsonSchema)]
pub struct Memory{
    pub swap_usage: u64,
    pub swap_usage_peak: u64,
    pub total: u64,
    pub usage: u64,
    pub usage_peak: u64,
}

#[derive(Deserialize, Serialize, Clone, Debug, Default, JsonSchema)]
pub struct Network(HashMap<String, InstanceInterface>);

#[derive(Deserialize, Serialize, Clone, Debug, Default, JsonSchema, PartialEq)]
pub struct InstanceInterface{
    pub addresses: Vec<Address>,
    pub host_name: String,
    pub hwaddr: String,
    pub mtu: u64,
    pub state: String,
    pub network: Option<String>,
    pub r#type: String,
    pub host_ifidx: Option<i32>,
    pub instance_ifidx: Option<i32>,
    pub host_mac: Option<String>,
}

#[derive(Deserialize, Serialize, Clone, Debug, Default, JsonSchema, PartialEq)]
pub struct Address{
    pub address: String,
    pub family: String,
    pub netmask: String,
    pub scope: String,
}

#[derive(Deserialize, Serialize, Clone, Debug, Default, JsonSchema, PartialEq)]
pub struct Counters{
    pub bytes_received: u64,
    pub bytes_sent: u64,
    pub errors_received: u64,
    pub errors_sent: u64,
    pub packets_dropped_inbound: u64,
    pub packets_dropped_outbound: u64,
    pub packets_received: u64,
    pub packets_sent: u64,
}

impl Instance{
    pub fn controller(client: client::Client, update_rx: mpsc::Receiver<ObjectRef<Instance>>) -> Controller<Instance> {
        let api = Api::<Instance>::default_namespaced(client.clone());
        Controller::new(api, Config::default())
        .reconcile_on(update_rx.map(|x| (x)))
        .watches(
            Api::<Instance>::all(client),
            Config::default(),
            |_object| {
                let object_list = Vec::new();
                object_list.into_iter()
            }
        )
    }

    pub async fn reconcile(g: Arc<Instance>, ctx: Arc<ResourceClient<Instance>>) ->  Result<Action, ReconcileError> {        
        info!("reconciling instance: {:?}", g.metadata.name);
        ctx.setup_finalizer(g, ctx.clone(), Instance::apply, Instance::cleanup).await
    }

    pub async fn cleanup(g: Arc<Instance>, ctx: Arc<ResourceClient<Instance>>) ->  Result<Action, ReconcileError> {      
        info!("cleaning up Instance: {:?}", g.metadata.name);
        if let Err(e) = ctx.lxd_client.as_ref().unwrap().delete(g.metadata.name.clone().unwrap()).await{
            warn!("failed to delete instance: {:?}", e);
            return Err(ReconcileError(e));
        }
        return Ok(Action::requeue(std::time::Duration::from_secs(5 * 60)))
    }

    pub async fn apply(g: Arc<Instance>, ctx: Arc<ResourceClient<Instance>>) ->  Result<Action, ReconcileError> {
        info!("reconciling instance: {:?}", g.metadata.name);
        let mut networks_ready = false;
        let mut routes_ready = false;
        let mut instance = match ctx.get::<Instance>(&g.metadata).await?{
            Some(mut instance) => {
                if instance.status.is_none(){
                    let mut instance_status = InstanceStatus::default();
                    instance_status.ready = false;
                    instance.status = Some(instance_status);
                    
                }
                instance
            },
            None => {
                warn!("instance not found, probably deleted");
                return Ok(Action::requeue(std::time::Duration::from_secs(5 * 60)))
            }
        };

        let instance_state = match ctx.lxd_client.as_ref().unwrap().status(instance.metadata.name.clone().unwrap()).await{
            Ok(instance_state) => {
                instance_state
            },
            Err(e) => {
                warn!("failed to get instance state: {:?}", e);
                return Err(ReconcileError(e));
            }
        };

        if let Some(instance_state) = instance_state{
            if let Some(instance_status) = &mut instance.status{
                let mut update = false;
                if instance_status.state != instance_state.status{
                    instance_status.state = instance_state.status.clone();
                    info!("instance state changed from {:?} to {:?}", instance_status.state, instance_state.status);
                    update = true;
                }
                if instance_status.networks != instance_state.network{
                    instance_status.networks = instance_state.network.clone();
                    info!("instance networks changed from {:?} to {:?}", instance_status.networks, instance_state.network);
                    update = true;
                }
                if update{
                    ctx.update_status(&instance).await?;
                }
                networks_ready = true;
            }            
            info!("instance state: {:?}", instance_state);
        } else {
            info!("instance not found");
            let instance_config = match Instance::define_instance(instance.clone(), ctx.clone()).await{
                Ok(instance_config) => {
                    instance_config
                },
                Err(e) => {
                    warn!("failed to define instance: {:?}", e);
                    return Err(ReconcileError(e));
                }
            };
            if let Err(e) = ctx.lxd_client.as_ref().unwrap().define(instance_config).await{
                warn!("failed to create instance: {:?}", e);
                return Err(ReconcileError(e));
            }
        }

        if let Some(routes) = &instance.spec.routes{
            if instance.status.as_mut().unwrap().routes.is_none(){
                instance.status.as_mut().unwrap().routes = Some(Vec::new());
            }
            let mut route_list = Vec::new();
            for route in routes{
                let mut route_status = RouteStatus::default();
                route_status.destination = route.destination.instance.clone();
                for next_hop in &route.next_hops{
                    let mut route_next_hop_status = RouteNextHopStatus::default();
                    let instance_metadata = ObjectMeta{
                        name: Some(next_hop.instance.clone()),
                        namespace: Some(instance.metadata.namespace.clone().unwrap()),
                        ..Default::default()
                    };
                    let next_hop_interface = match find_instance_interface(instance_metadata.clone(), next_hop.network.clone(), ctx.clone()).await{
                        Ok(next_hop_interface) => {
                            next_hop_interface
                        },
                        Err(e) => {
                            warn!("failed to find instance interface: {:?}", e);
                            return Err(ReconcileError(e));
                        }
                    };

                    route_next_hop_status.ip = next_hop_interface.addresses[0].address.clone();
                    route_next_hop_status.mac = next_hop_interface.hwaddr.clone();
                    route_status.next_hops.push(route_next_hop_status);
                }
                route_list.push(route_status);
                
            }
            if route_list != instance.status.as_ref().unwrap().routes.as_ref().unwrap().clone(){
                instance.status.as_mut().unwrap().routes = Some(route_list);
                ctx.update_status(&instance).await?;
            }
            routes_ready = true;
        } else {
            routes_ready = true;
        }
        if networks_ready && routes_ready && instance.status.as_ref().unwrap().ready == false {
            instance.status.as_mut().unwrap().ready = true;
            ctx.update_status(&instance).await?;
        }
        Ok(Action::requeue(std::time::Duration::from_secs(5 * 60)))
    }
    pub fn error_policy(_g: Arc<Instance>, error: &ReconcileError, _ctx: Arc<ResourceClient<Instance>>) -> Action {
        warn!("reconcile failed: {:?}", error);
        Action::requeue(Duration::from_secs(5 * 60))
    }

    pub async fn release_ip(ctx: Arc<ResourceClient<Instance>>, ip: String, network: String, namespace: &String) -> anyhow::Result<()> {
        let network_metadata = ObjectMeta{
            name: Some(network),
            namespace: Some(namespace.clone()),
            ..Default::default()
        };
        let mut network = match ctx.get::<network::Network>(&network_metadata).await?{
            Some(network) => {
                network
            },
            None => {
                warn!("network not found: {:?}", network_metadata);
                return Ok(());
            }
        };
        if let Some(mut network_status) = network.status{
            network_status.unused.sort();
            let ip: std::net::Ipv4Addr = ip.parse()?;
            let ip_dec = u32::from(ip);
            network_status.unused.push(ip_dec);
            network.status = Some(network_status);
            if let Err(e) = ctx.update_status(&network).await{
                return Err(anyhow::anyhow!("failed to update network status: {:?}", e));
            }
        } else {
            return Err(anyhow::anyhow!("network status not found"));
        }
        Ok(())
    }

    pub async fn define_instance(g: Instance, ctx: Arc<ResourceClient<Instance>>) -> anyhow::Result<InstanceConfig> {
        let mut interfaces_map = HashMap::new();
        for interface in &g.spec.interfaces{
            let mgmt = match &interface.mgmt{
                Some(mgmt) => {
                    *mgmt
                }
                None => {
                    false
                }
            };
            if !mgmt{
                let network_metadata = ObjectMeta{
                    name: interface.network.name.clone(),
                    namespace: Some(g.metadata.namespace.clone().unwrap()),
                    ..Default::default()
                };
                let mut network = match ctx.get::<network::Network>(&network_metadata).await?{
                    Some(network) => {
                        network
                    },
                    None => {
                        warn!("network not found: {:?}", network_metadata);
                        continue;
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
                        return Err(anyhow::anyhow!("failed to update network status: {:?}", e));
                    }
                    let ip_net: ipnet::Ipv4Net = network.spec.subnet.parse()?;
                    let prefix_len = ip_net.prefix_len();
                    (ip, prefix_len)
                } else {
                    return Err(anyhow::anyhow!("network status not found"));
                };
                let mac = generate_mac_address();
                let interface_config_type = lxdmanager::InterfaceConfigType::Network{
                    ipv4: ip,
                    prefix_len,
                    mac: mac.clone(),
                };
                interfaces_map.insert(interface.name.clone(), interface_config_type);
            } else {
                let interface_config_type = lxdmanager::InterfaceConfigType::Mgmt { network: interface.network.name.as_ref().unwrap().clone() };
                interfaces_map.insert(interface.name.clone(), interface_config_type);
            }
        }
        let instance_config = InstanceConfig{
            instance: g.clone(),
            interfaces: interfaces_map,
        };
        Ok(instance_config)
    }
}

async fn find_instance_interface(instance_meta_data: ObjectMeta, network_name: String, ctx: Arc<ResourceClient<Instance>>) -> anyhow::Result<InstanceInterface> {
    let instance = match ctx.get::<Instance>(&instance_meta_data).await?{
        Some(instance) => {
            instance
        },
        None => {
            warn!("instance not found: {:?}", instance_meta_data);
            return Err(anyhow::anyhow!("instance not found"));
        }
    };

    let mut interface_name = None;
    for interface in &instance.spec.interfaces{
        if interface.network.name.as_ref().unwrap().to_string() == network_name{
            interface_name = Some(interface.name.clone());
            break;
        }
    }

    let interface_name = if let Some(interface_name) = interface_name{
        interface_name
    } else {
        warn!("interface not found: {:?}", network_name);
        return Err(anyhow::anyhow!("interface not found"));
    };

    let instance_status = if let Some(instance_status) = &instance.status{
        instance_status
    } else {
        warn!("instance status not found: {:?}", instance);
        return Err(anyhow::anyhow!("instance status not found"));
    };
    let instance_interface = if let Some(instance_interface) = instance_status.networks.as_ref().unwrap().get(&interface_name){
        instance_interface.clone()
    } else {
        warn!("instance interface not found: {:?}", interface_name);
        return Err(anyhow::anyhow!("instance interface not found"));
    };
    Ok(instance_interface)
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