use std::{sync::Arc, time::Duration, collections::{BTreeMap, HashMap}};

use anyhow::anyhow;
use kube::{CustomResource, runtime::{Controller, watcher::Config, controller::Action}, Api, client, Client};
use serde::{Deserialize, Serialize};
use garde::Validate;
use schemars::JsonSchema;
use tracing::{warn, info};
use ipnet;
use kube_runtime::finalizer;


use crate::{instance::instance::Instance, resource::resource::{ReconcileError, ResourceClient}};
use crate::resource::resource::Context;

#[derive(CustomResource, Deserialize, Serialize, Clone, Debug, Validate, JsonSchema)]
#[kube(group = "virt.dev", version = "v1", kind = "Network", namespaced)]
#[kube(status = "NetworkStatus")]
#[kube(printcolumn = r#"{"name":"Ip", "jsonPath": ".spec.metadata.team", "type": "string"}"#)]
#[serde(rename_all = "camelCase")]
pub struct NetworkSpec {
    #[garde(skip)]
    pub subnet: String
    
}

#[derive(Deserialize, Serialize, Clone, Debug, Default, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct NetworkStatus {
    pub unused: Vec<u32>,
    pub last_ip: u32,
    pub gateway: String,
}

impl Network{
    pub fn controller(client: client::Client) -> Controller<Network> {
        let api = Api::<Network>::all(client.clone());
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
    pub async fn reconcile(g: Arc<Network>, ctx: Arc<ResourceClient<Network>>) ->  Result<Action, ReconcileError> {        
        info!("reconciling network: {:?}", g.metadata.name);
        ctx.setup_finalizer(g, ctx.clone(), Network::apply, Network::cleanup).await?; 
        Ok(Action::await_change())
    }
    
    pub async fn cleanup(g: Arc<Network>, ctx: Arc<ResourceClient<Network>>) ->  Result<Action, ReconcileError> {      
        info!("cleaning up network: {:?}", g.metadata.name);
        Ok(Action::await_change())
    }

    pub async fn apply(g: Arc<Network>, ctx: Arc<ResourceClient<Network>>) ->  Result<Action, ReconcileError> {      
        info!("reconciling network: {:?}", g.metadata.name);
        let mut network = match ctx.get::<Network>(&g.metadata).await?{
            Some(network) => {
                network
            },
            None => {
                warn!("network not found, probably deleted");
                return Ok(Action::await_change());
            }
        };
        if network.status.is_none(){
            let ip_net: ipnet::Ipv4Net = network.spec.subnet.parse().unwrap();
            let ip_net_octets = ip_net.network().octets();
            let ip_net_dec = u32::from_be_bytes(ip_net_octets);
            let gateway = ip_net_dec + 1;
            let last_ip = std::net::Ipv4Addr::from(gateway);
            let mut status = NetworkStatus::default();
            status.gateway = last_ip.to_string();
            status.last_ip = gateway;
            status.unused = Vec::new();
            network.status = Some(status);
            ctx.update_status(&network).await?;
        }
        Ok(Action::await_change())
    }
    pub fn error_policy(_g: Arc<Network>, error: &ReconcileError, _ctx: Arc<ResourceClient<Network>>) -> Action {
        warn!("reconcile failed: {:?}", error);
        Action::requeue(Duration::from_secs(1))
    }
}
