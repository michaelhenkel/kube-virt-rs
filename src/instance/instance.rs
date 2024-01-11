use std::{sync::Arc, time::Duration, collections::HashMap};
use k8s_openapi::api::core::v1::LocalObjectReference;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::OwnerReference;
use kube::{
    CustomResource,
    runtime::{Controller, watcher::Config, controller::Action},
    Api,
    client, core::ObjectMeta
};
use kube_runtime::reflector::ObjectRef;
use serde::{Deserialize, Serialize};
use garde::Validate;
use schemars::JsonSchema;
use tracing::{warn, info};
use futures::channel::mpsc;
use futures::stream::StreamExt;
use crate::flowtable::flowtable;
use crate::resource::resource::{ReconcileError, ResourceClient};


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
pub struct InstanceConfig{
    #[serde(skip_serializing_if = "Option::is_none")]
    pub devices: Option<HashMap<String, InstanceInterface>>,
    pub status: String,
}

#[derive(Deserialize, Serialize, Clone, Debug, Default, JsonSchema)]
pub struct InstanceState{
    #[serde(skip_serializing_if = "Option::is_none")]
    pub network: Option<HashMap<String, InstanceInterface>>,
    pub status: String,
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
    pub addresses: Option<Vec<Address>>,
    pub host_name: String,
    pub hwaddr: String,
    pub mtu: Option<u64>,
    pub state: Option<String>,
    pub network: Option<String>,
    pub r#type: String,
    pub host_ifidx: Option<i32>,
    pub instance_ifidx: Option<i32>,
    pub host_mac: Option<String>,
    pub name: Option<String>,
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
        return Ok(Action::await_change())
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

         
        match ctx.get::<flowtable::Flowtable>(&g.metadata).await?{
            Some(_) => {},
            None => {
                let flow_table = flowtable::Flowtable{
                    metadata: ObjectMeta{
                        name: instance.metadata.name.clone(),
                        namespace: instance.metadata.namespace.clone(),
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
                    }, 
                    spec: flowtable::FlowtableSpec{
                        flow_table_type: flowtable::FlowTableType::Instance,
                    },
                    status: None,
                };
                ctx.create(&flow_table).await?;
            }
        }

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
                    instance_status.state = instance_state.clone();
                    if instance_state == "Running"{
                        instance_status.ready = true;
                    } else {
                        instance_status.ready = false;
                    }
                    ctx.update_status(&instance).await?;
                }
            }            
        } else{
            if let Err(e) = ctx.lxd_client.as_ref().unwrap().define(instance.clone()).await{
                warn!("failed to create instance: {:?}", e);
                return Err(ReconcileError(e));
            }
            instance.status.as_mut().unwrap().phase = "Defined".to_string();
            ctx.update_status(&instance).await?;
        }
        Ok(Action::requeue(std::time::Duration::from_secs(5 * 300)))
    }
    pub fn error_policy(_g: Arc<Instance>, error: &ReconcileError, _ctx: Arc<ResourceClient<Instance>>) -> Action {
        warn!("reconcile failed: {:?}", error);
        Action::requeue(Duration::from_secs(5))
    }
}