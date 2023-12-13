use std::{sync::Arc, time::Duration};

use k8s_openapi::api::core::v1::LocalObjectReference;
use kube::{
    CustomResource,
    runtime::{Controller, watcher::Config, controller::Action},
    Api,
    client, core::{Object, ObjectMeta}
};
use serde::{Deserialize, Serialize};
use garde::Validate;
use schemars::JsonSchema;
use tracing::warn;
use ipnet;

use crate::{resource::resource::{ReconcileError, ResourceClient}, interface::interface::InterfaceSpec, network::network::Network};
use crate::resource::resource::Context;

#[derive(CustomResource, Deserialize, Serialize, Clone, Debug, Validate, JsonSchema)]
#[kube(group = "virt.dev", version = "v1", kind = "Ipaddress", namespaced)]
#[kube(status = "IpaddressStatus")]
#[kube(printcolumn = r#"{"name":"Ip", "jsonPath": ".spec.metadata.team", "type": "string"}"#)]
#[serde(rename_all = "camelCase")]
pub struct IpaddressSpec {
    #[garde(skip)]
    pub network: LocalObjectReference,
}

#[derive(Deserialize, Serialize, Clone, Debug, Default, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct IpaddressStatus {
    pub ip: Option<String>,
}

impl Ipaddress{
    pub fn controller(client: client::Client) -> Controller<Ipaddress> {
        let api = Api::<Ipaddress>::default_namespaced(client.clone());
        Controller::new(api, Config::default())
        .watches(
            Api::<Ipaddress>::all(client),
            Config::default(),
            |object| {
                let object_list = Vec::new();
                object_list.into_iter()
            }
        )
    }
    pub async fn reconcile(g: Arc<Ipaddress>, ctx: Arc<ResourceClient<Ipaddress>>) ->  Result<Action, ReconcileError> {
        let mut ipaddress = match ctx.get::<Ipaddress>(&g.metadata).await?{
            Some(ipaddress) => {
                ipaddress
            },
            None => {
                warn!("ipaddress not found, probably deleted");
                return Ok(Action::await_change());
            }
        };
        if ipaddress.status.is_none(){
            let network_metadata = ObjectMeta{
                name: ipaddress.spec.network.name.clone(),
                namespace: ipaddress.metadata.namespace.clone(),
                ..Default::default()
            };
            let mut network = match ctx.get::<Network>(&network_metadata).await?{
                Some(network) => {
                    network
                },
                None => {
                    warn!("network not found, probably deleted");
                    return Ok(Action::await_change());
                }
            };
            if let Some(mut network_status) = network.status{
                network_status.unused.sort();
                if let Some(mut ip_dec) = network_status.unused.pop(){
                    ip_dec += 1;
                    let ip = std::net::Ipv4Addr::from(ip_dec);
                    let mut status = IpaddressStatus::default();
                    status.ip = Some(ip.to_string());
                    ipaddress.status = Some(status);
                    ctx.update_status(&ipaddress).await?;
                    if network_status.unused.len() == 0{
                        network_status.last_ip = ip_dec;
                    }
                    network.status = Some(network_status);
                    ctx.update_status(&network).await?;
                } else {
                    let ip_dec = network_status.last_ip + 1;
                    let ip = std::net::Ipv4Addr::from(ip_dec);
                    let mut status = IpaddressStatus::default();
                    status.ip = Some(ip.to_string());
                    ipaddress.status = Some(status);
                    
                    network_status.last_ip = ip_dec;
                    network.status = Some(network_status);
                    match ctx.update_status(&network).await{
                        Ok(_) => {
                            ctx.update_status(&ipaddress).await?;
                        },
                        Err(_) => {
                            return Ok(Action::requeue(Duration::from_secs(1)));
                        }
                    }
                }
            
            }
        }
        Ok(Action::await_change())
    }
    pub fn error_policy(_g: Arc<Ipaddress>, error: &ReconcileError, _ctx: Arc<ResourceClient<Ipaddress>>) -> Action {
        warn!("reconcile failed: {:?}", error);
        Action::requeue(Duration::from_secs(5 * 60))
    }
}