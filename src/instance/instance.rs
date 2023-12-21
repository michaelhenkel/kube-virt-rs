use std::collections::BTreeMap;
use std::{sync::Arc, time::Duration, collections::HashMap, any};
use k8s_openapi::{chrono::Local, api::core::v1::LocalObjectReference};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::OwnerReference;
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
use crate::interface::interface;
use crate::{resource::resource::{ReconcileError, ResourceClient}, instance, lxdmanager::lxdmanager};
//use crate::lxdmanager::lxdmanager::InstanceConfig;
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
    pub state: String,
    pub networks: Option<HashMap<String, InstanceInterface>>,
    pub routes: Option<Vec<RouteStatus>>,
    pub ready: bool,
    pub phase: String,
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
            Api::<interface::Interface>::all(client),
            Config::default(),
            |interface| {
                let mut object_list = Vec::new();
                if let Some(owner_ref) = &interface.metadata.owner_references{
                    for owner_ref in owner_ref{
                        if owner_ref.kind == "Instance"{
                            if let Some(labels) = &interface.metadata.labels{
                                if let Some(instance_name) = labels.get("virt.dev/instance"){
                                    let instance_metadata = ObjectMeta{
                                        name: Some(instance_name.clone()),
                                        namespace: Some(interface.metadata.namespace.clone().unwrap()),
                                        ..Default::default()
                                    };
                                    let object_ref = ObjectRef::new(instance_name).within(&interface.metadata.namespace.clone().unwrap());
                                    object_list.push(object_ref);
                                }
                            }
                        }
                    }
                }
                object_list.into_iter()
            }
        )
    }

    pub async fn reconcile(g: Arc<Instance>, ctx: Arc<ResourceClient<Instance>>) ->  Result<Action, ReconcileError> {        
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
        let mut instance = match ctx.get::<Instance>(&g.metadata).await?{
            Some(mut instance) => {
                if instance.status.is_none(){
                    let mut instance_status = InstanceStatus::default();
                    instance_status.ready = false;
                    instance_status.phase = "Created".to_string();
                    instance.status = Some(instance_status);
                    if let Some(res) = ctx.update_status(&instance).await?{
                        instance = res;
                    }
                }
                instance
            },
            None => {
                warn!("instance not found, probably deleted");
                return Ok(Action::requeue(std::time::Duration::from_secs(5 * 300)))
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
                if instance_status.state != instance_state{
                    info!("instance state changed from {:?} to {:?}", instance_status.state, instance_state);
                    instance_status.state = instance_state.clone();
                    if let Some(res) = ctx.update_status(&instance).await?{
                        instance = res;
                    }
                }
            }            
        } else {
            info!("instance not found");
            if let Err(e) = ctx.lxd_client.as_ref().unwrap().define(instance.clone()).await{
                warn!("failed to create instance: {:?}", e);
                return Err(ReconcileError(e));
            }
            instance.status.as_mut().unwrap().phase = "Defined".to_string();
            if let Some(res) = ctx.update_status(&instance).await?{
                instance = res;
            }
            for (idx, intf) in instance.spec.interfaces.iter().enumerate(){
                let intf_meta = ObjectMeta{
                    name: Some(format!("{}-{}", instance.metadata.name.as_ref().unwrap().clone(),intf.name.clone())),
                    namespace: Some(instance.metadata.namespace.clone().unwrap()),
                    owner_references: Some(vec![
                        OwnerReference{
                            api_version: "virt.dev/v1".to_string(),
                            kind: "Instance".to_string(),
                            name: instance.metadata.name.as_ref().unwrap().clone(),
                            uid: instance.metadata.uid.as_ref().unwrap().clone(),
                            ..Default::default()
                        }
                    ]),
                    labels: Some(BTreeMap::from([(
                        "virt.dev/instance".to_string(),
                        instance.metadata.name.as_ref().unwrap().clone(),

                    )])),
                    ..Default::default()
                };
                let res = ctx.get::<interface::Interface>(&intf_meta).await?;
                if res.is_none(){
                    let intf = interface::Interface{
                        metadata: intf_meta,
                        spec: interface::InterfaceSpec{
                            name: intf.name.clone(),
                            network: intf.network.clone(),
                            mtu: intf.mtu,
                            mgmt: intf.mgmt,
                            pci_idx: idx as u32,
                        },
                        status: None,
                    };
                    ctx.create(&intf).await?;
                }
            }
        }

        if instance.status.as_ref().unwrap().state == "Stopped".to_string() && instance.status.as_ref().unwrap().phase == "Defined".to_string(){
            let mut all_interfaces_defined = true;
            for intf in &instance.spec.interfaces{
                let intf_meta = ObjectMeta{
                    name: Some(format!("{}-{}", instance.metadata.name.as_ref().unwrap().clone(),intf.name.clone())),
                    namespace: Some(instance.metadata.namespace.clone().unwrap()),
                    owner_references: Some(vec![
                        OwnerReference{
                            api_version: "virt.dev/v1".to_string(),
                            kind: "Instance".to_string(),
                            name: instance.metadata.name.as_ref().unwrap().clone(),
                            uid: instance.metadata.uid.as_ref().unwrap().clone(),
                            ..Default::default()
                        }
                    ]),
                    ..Default::default()
                };
                if let Some(intf) = ctx.get::<interface::Interface>(&intf_meta).await?{
                    if let Some(intf_status) = intf.status{
                        if !intf_status.defined{
                            all_interfaces_defined = false;
                            break;
                        }
                    }
                }
            }
            if all_interfaces_defined{
                info!("all interfaces defined, starting instance");
                if let Err(e) = ctx.lxd_client.as_ref().unwrap().start(instance.clone()).await{
                    warn!("failed to start instance: {:?}", e);
                    return Err(ReconcileError(e));
                }
                instance.status.as_mut().unwrap().phase = "Started".to_string();
                ctx.update_status(&instance).await?;
            }
        }
        Ok(Action::requeue(std::time::Duration::from_secs(5 * 300)))
    }
    pub fn error_policy(_g: Arc<Instance>, error: &ReconcileError, _ctx: Arc<ResourceClient<Instance>>) -> Action {
        warn!("reconcile failed: {:?}", error);
        Action::requeue(Duration::from_secs(5))
    }

    async fn release_ip(ctx: Arc<ResourceClient<Instance>>, ip: String, network: String, namespace: &String) -> anyhow::Result<()> {
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
}