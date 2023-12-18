pub mod interface;
pub mod instance;
pub mod ipaddress;
pub mod network;
pub mod resource;
pub mod lxdmanager;

use std::sync::Arc;

use ipaddress::ipaddress::Ipaddress;
use lxdmanager::lxdmanager::LxdManager;
use network::network::Network;
use resource::resource::{ResourceManager, Context, ResourceClient};
use interface::interface::Interface;
use instance::instance::Instance;
use futures::channel::mpsc;

use anyhow::Result;
use kube::{
    core::crd::CustomResourceExt,
    Client,
};


#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    let client = Client::try_default().await?;

    let resource_mgr = ResourceManager::new(client.clone());

    resource_mgr.create(Instance::crd(), Instance::crd_name()).await?;
    resource_mgr.create(Network::crd(), Network::crd_name()).await?;

    let mut join_list = Vec::new();

    let resource_client: ResourceClient<Network> = ResourceClient::new(client.clone(), None);
    let cl = client.clone();
    let r_m = resource_mgr.clone();
    join_list.push(tokio::spawn(async move {
        r_m.watch(Network::controller(cl.clone()),Network::reconcile, Network::error_policy, resource_client).await?;
        Ok::<(), anyhow::Error>(())
    }));

    let (update_tx, update_rx) = mpsc::channel(0);
    let mut lxd_manager = LxdManager::new(update_tx.clone());
    let lxd_client = lxd_manager.client.clone();
    join_list.push(tokio::spawn(async move {
        lxd_manager.start().await?;
        Ok::<(), anyhow::Error>(())
    }));

    let resource_client: ResourceClient<Instance> = ResourceClient::new(client.clone(), Some(lxd_client.clone()));
    let cl = client.clone();
    let r_m = resource_mgr.clone();
    join_list.push(tokio::spawn(async move {
        r_m.watch(Instance::controller(cl.clone(), update_rx),Instance::reconcile, Instance::error_policy,resource_client).await?;
        Ok::<(), anyhow::Error>(())
    }));

    futures::future::join_all(join_list).await;
    Ok(())
}