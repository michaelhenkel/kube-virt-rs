pub mod interface;
pub mod instance;
pub mod ipaddress;
pub mod network;
pub mod resource;

use std::sync::Arc;

use ipaddress::ipaddress::Ipaddress;
use network::network::Network;
use resource::resource::{ResourceManager, Context, ResourceClient};
use interface::interface::Interface;
use instance::instance::Instance;

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

    resource_mgr.create(Interface::crd(), Interface::crd_name()).await?;
    resource_mgr.create(Instance::crd(), Instance::crd_name()).await?;
    resource_mgr.create(Ipaddress::crd(), Ipaddress::crd_name()).await?;
    resource_mgr.create(Network::crd(), Network::crd_name()).await?;

    let mut join_list = Vec::new();

    let ctx = Arc::new(Context{client: client.clone()});
    let resource_client: ResourceClient<Interface> = ResourceClient::new(client.clone());
    let cl = client.clone();
    let r_m = resource_mgr.clone();
    join_list.push(tokio::spawn(async move {
        r_m.watch(Interface::controller(cl.clone()),Interface::reconcile, Interface::error_policy, resource_client).await?;
        Ok::<(), anyhow::Error>(())
    }));

    let ctx = Arc::new(Context{client: client.clone()});
    let resource_client: ResourceClient<Instance> = ResourceClient::new(client.clone());
    let cl = client.clone();
    let r_m = resource_mgr.clone();
    join_list.push(tokio::spawn(async move {
        r_m.watch(Instance::controller(cl.clone()),Instance::reconcile, Instance::error_policy,resource_client).await?;
        Ok::<(), anyhow::Error>(())
    }));

    let ctx = Arc::new(Context{client: client.clone()});
    let resource_client: ResourceClient<Network> = ResourceClient::new(client.clone());
    let cl = client.clone();
    let r_m = resource_mgr.clone();
    join_list.push(tokio::spawn(async move {
        r_m.watch(Network::controller(cl.clone()),Network::reconcile, Network::error_policy, resource_client).await?;
        Ok::<(), anyhow::Error>(())
    }));

    let ctx = Arc::new(Context{client: client.clone()});
    let resource_client: ResourceClient<Ipaddress> = ResourceClient::new(client.clone());
    let cl = client.clone();
    let r_m = resource_mgr.clone();
    join_list.push(tokio::spawn(async move {
        r_m.watch(Ipaddress::controller(cl.clone()),Ipaddress::reconcile, Ipaddress::error_policy, resource_client).await?;
        Ok::<(), anyhow::Error>(())
    }));

    futures::future::join_all(join_list).await;
    Ok(())
}