pub mod interface;
pub mod instance;
pub mod ipaddress;
pub mod network;
pub mod resource;
pub mod lxdmanager;

use std::sync::Arc;

use ipaddress::ipaddress::Ipaddress;
use lxdmanager::lxdmanager::{LxdManager, LxdMonitor};
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
    resource_mgr.create(Instance::crd(), Instance::crd_name()).await?;
    resource_mgr.create(Interface::crd(), Interface::crd_name()).await?;

    let mut join_list = Vec::new();

    let resource_client: ResourceClient<Network> = ResourceClient::new(client.clone(), None);
    let cl = client.clone();
    let r_m = resource_mgr.clone();
    join_list.push(tokio::spawn(async move {
        r_m.watch(Network::controller(cl.clone()),Network::reconcile, Network::error_policy, resource_client).await?;
        Ok::<(), anyhow::Error>(())
    }));

    let mut lxd_monitor = LxdMonitor::new();
    let lxd_monitor_client = lxd_monitor.client.clone();
    join_list.push(tokio::spawn(async move {
        lxd_monitor.run().await?;
        Ok::<(), anyhow::Error>(())
    }));

    let (instance_update_tx, instance_update_rx) = mpsc::channel(1);
    let (interface_update_tx, interface_update_rx) = mpsc::channel(1);
    
    let mut lxd_manager = LxdManager::new(instance_update_tx.clone(), interface_update_tx.clone(), lxd_monitor_client.clone());
    let lxd_client = lxd_manager.client.clone();
    join_list.push(tokio::spawn(async move {
        lxd_manager.start().await?;
        Ok::<(), anyhow::Error>(())
    }));



    let resource_client: ResourceClient<Instance> = ResourceClient::new(client.clone(), Some(lxd_client.clone()));
    let cl = client.clone();
    let r_m = resource_mgr.clone();
    join_list.push(tokio::spawn(async move {
        r_m.watch(Instance::controller(cl.clone(), instance_update_rx),Instance::reconcile, Instance::error_policy,resource_client).await?;
        Ok::<(), anyhow::Error>(())
    }));

    let resource_client: ResourceClient<Interface> = ResourceClient::new(client.clone(), Some(lxd_client.clone()));
    let cl = client.clone();
    let r_m = resource_mgr.clone();
    join_list.push(tokio::spawn(async move {
        r_m.watch(Interface::controller(cl.clone(), interface_update_rx),Interface::reconcile, Interface::error_policy,resource_client).await?;
        Ok::<(), anyhow::Error>(())
    }));


    futures::future::join_all(join_list).await;
    Ok(())
}