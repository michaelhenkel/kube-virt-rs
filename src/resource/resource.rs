use core::future::IntoFuture;
use futures::StreamExt;
use futures::TryFuture;
use k8s_openapi::NamespaceResourceScope;
use kube::core::ObjectMeta;
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::hash::Hash;
use std::sync::Arc;
use kube::api::PostParams;
use std::fmt::Debug;
use tokio::time::sleep;
use std::time::Duration;
use anyhow::Result;
use k8s_openapi::apiextensions_apiserver::pkg::apis::apiextensions::v1::CustomResourceDefinition;
use kube::{Client, Api, api::DeleteParams};
use kube::{
    Error,
    ResourceExt,
    runtime::{
        watcher, 
        WatchStreamExt,
        controller::{Action, Controller},
        watcher::Config,
        reflector::ObjectRef,
    }
};
use async_trait::async_trait;

use tracing::*;
use futures::stream::TryStreamExt;

#[derive(Debug)]
pub struct ReconcileError(pub anyhow::Error);
impl std::error::Error for ReconcileError {

}
impl std::fmt::Display for ReconcileError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f,"ReconcileError: {}", self.0)
    }
}


#[derive(Clone)]
pub struct ResourceManager{
    client: Client
}

pub trait Resource{
    fn name(&self) -> String;
}

#[derive(Clone)]
pub struct Context {
    pub client: Client,
}

#[derive(Clone)]
pub struct ResourceClient<T> {
    pub client: Client,
    pub api: Api<T>,
}

impl<T> ResourceClient<T>
where
T: kube::Resource<Scope = NamespaceResourceScope> + serde::Serialize,
<T as kube::Resource>::DynamicType: Default,
T: Clone + DeserializeOwned + Debug,
{
    pub fn new(client: Client) -> Self {
        let api = Api::<T>::all(client.clone());
        Self {
            client,
            api,
        }
    }
    pub async fn get<R: kube::Resource>(&self, metadata: &ObjectMeta) -> Result<Option<R>, ReconcileError>
    where
        R: kube::Resource<Scope = NamespaceResourceScope>,
        <R as kube::Resource>::DynamicType: Default,
        R: Clone + DeserializeOwned + Debug,
    {
        let namespace = metadata.namespace.as_ref().unwrap();
        let name = metadata.name.as_ref().unwrap();
        let res_api: Api<R> = Api::namespaced(self.client.clone(), namespace);
        let res = match res_api.get(name).await{
            Ok(res) => {
                info!("Found resource: {:?}", res.meta().name.as_ref().unwrap());
                Some(res)
            },
            Err(e) => {
                if is_not_found(&e){
                    None
                } else {
                    return Err(ReconcileError(e.into()));
                }
            },
        };
        Ok(res)
    }
    pub async fn create<R: kube::Resource>(&self, t: &R) -> Result<Option<R>, ReconcileError>
    where
        R: kube::Resource<Scope = NamespaceResourceScope> + serde::Serialize,
        <R as kube::Resource>::DynamicType: Default,
        R: Clone + DeserializeOwned + Debug,
    {
        info!("Creating {:?}", t.meta().name.as_ref().unwrap());
        let res_api: Api<R> = Api::namespaced(self.client.clone(), t.meta().namespace.as_ref().unwrap());
        let res = match res_api.create(&PostParams::default(), &t).await{
            Ok(res) => {
                Some(res)
            },
            Err(e) => {
                if is_not_found(&e){
                    None
                } else {
                    return Err(ReconcileError(e.into()));
                }
            },
        };
        Ok(res)
    }
    
    pub async fn update_status<R: kube::Resource>(&self, t: &R) -> Result<Option<R>, ReconcileError>
    where
        R: kube::Resource<Scope = NamespaceResourceScope>,
        <R as kube::Resource>::DynamicType: Default,
        R: Clone + DeserializeOwned + Debug + Serialize,
    {
        info!("Updating Status {:?}", t.meta().name.as_ref().unwrap());
        let patch = serde_json::to_vec(&t).unwrap();
        let params = PostParams::default();
        let res_api: Api<R> = Api::namespaced(self.client.clone(), t.meta().namespace.as_ref().unwrap());
        let res = match res_api.replace_status(t.clone().meta().name.as_ref().unwrap(), &params, patch).await{
            Ok(res) => {
                Some(res)
            },
            Err(e) => {
                if is_not_found(&e){
                    info!("status not found: {:?}", e);
                    None
                } else {
                    return Err(ReconcileError(e.into()));
                }
            },
        };
        Ok(res)
    }
}

impl ResourceManager{
    pub fn new(client: Client) -> Self {
        Self{
            client
        }
    }
    pub async fn create(&self, crd: CustomResourceDefinition, crd_name: &str) -> Result<()> 
    {
        let crds: Api<CustomResourceDefinition> = Api::all(self.client.clone());
        let dp = DeleteParams::default();
        let _ = crds.delete(crd_name, &dp).await.map(|res| {
            res.map_left(|o| {
                info!(
                    "Deleting {}: ({:?})",
                    o.name_any(),
                    o.status.unwrap().conditions.unwrap().last()
                );
            })
            .map_right(|s| {
                // it's gone.
                info!("Deleted crd: ({:?})", s);
            })
        });
        // Wait for the delete to take place (map-left case or delete from previous run)
        sleep(Duration::from_secs(1)).await;
    
        // Create the CRD so we can create Foos in kube
        info!("Creating CRD: {}", serde_json::to_string_pretty(&crd)?);
        let pp = PostParams::default();
        //let patch_params = PatchParams::default();
        match crds.create(&pp, &crd).await {
            Ok(o) => {
                info!("Created {} ({:?})", o.name_any(), o.status.unwrap());
                debug!("Created CRD: {:?}", o.spec);
            }
            Err(kube::Error::Api(ae)) => assert_eq!(ae.code, 409), // if you skipped delete, for instance
            Err(e) => return Err(e.into()),                        // any other case is probably bad
        }
        // Wait for the api to catch up
        sleep(Duration::from_secs(1)).await;
        Ok(())
    }

    /*
    pub async fn watch<'a, R>(&self, watch_fn: fn(r: R)) -> Result<()> 
    where 
    R: kube::Resource, 
        <R as kube::Resource>::DynamicType: Default, 
        R: kube::Resource<Scope = NamespaceResourceScope> + for<'de> serde::Deserialize<'de> + Clone + Debug + Send + Sync + 'static,
    {
        let api = Api::<R>::default_namespaced(self.client.clone());
        let use_watchlist = std::env::var("WATCHLIST").map(|s| s == "1").unwrap_or(false);
        let wc = if use_watchlist {
            watcher::Config::default().streaming_lists()
        } else {
            watcher::Config::default()
        };
        watcher(api, wc)
            .applied_objects()
            .default_backoff()
            .try_for_each(|p| async move {
                info!("saw {}", p.name_any());
                watch_fn(p);
                Ok(())
            })
        .await?;
        Ok(())
    }
    */

    pub async fn watch<'a, R, ReconcilerFut>(&self, ctrl: Controller<R>,
        reconciler: impl FnMut(Arc<R>, Arc<ResourceClient<R>>) -> ReconcilerFut,
        error_policy: impl Fn(Arc<R>, &ReconcilerFut::Error, Arc<ResourceClient<R>>) -> Action,
        resource_client: ResourceClient<R>,
    ) -> Result<()> 
    where
    <R as kube::Resource>::DynamicType: Default + Hash + Eq + Clone + std::fmt::Debug + Unpin,
    R: kube::Resource<Scope = NamespaceResourceScope>
        + for<'de> serde::Deserialize<'de>
        + Clone
        + Debug
        + Send
        + Sync
        + 'static,
    ReconcilerFut: TryFuture<Ok = Action> + std::marker::Send + 'static,
    ReconcilerFut::Error: std::error::Error + 'static,
    <ReconcilerFut as TryFuture>::Error: std::marker::Send
    {
        ctrl.
            run(reconciler, error_policy, Arc::new(resource_client))
            .for_each(|res| async move {
                match res {
                    Ok(o) => info!("reconciled {:?}", o),
                    Err(e) => warn!("reconcile failed: {:?}", e),
                }
            })
            .await;
        Ok(())
    }
}

/*
pub async fn get<T: kube::Resource>(metadata: &ObjectMeta, client: Client) -> Result<Option<T>, ReconcileError>
where
T: kube::Resource<Scope = NamespaceResourceScope>,
<T as kube::Resource>::DynamicType: Default,
T: Clone + DeserializeOwned + Debug,
{
    let namespace = metadata.namespace.as_ref().unwrap();
    let name = metadata.name.as_ref().unwrap();
    let res_api: Api<T> = Api::namespaced(client.clone(), namespace);
    let res = match res_api.get(name).await{
        Ok(res) => {
            info!("Found resource: {:?}", res.meta().name.as_ref().unwrap());
            Some(res)
        },
        Err(e) => {
            if is_not_found(&e){
                None
            } else {
                return Err(ReconcileError(e.into()));
            }
        },
    };
    Ok(res)
}
*/

pub fn is_not_found(e: &Error) -> bool {
    match e{
        kube::Error::Api(ae) => {
            match ae{
                kube::error::ErrorResponse { status: _, message: _, reason: r, code: _ } => {
                    match r.as_str(){
                        "NotFound" => {
                            info!("Resource not found: {:?}", e);
                            return true
                        },
                        _ => {
                            return false
                        },
                    }
                },
                _ => {
                    return false
                },
            }
        },
        _ => {
            return false
        },
    }
}

/*
pub async fn create<T: kube::Resource>(t: &T, client: Client) -> Result<Option<T>, ReconcileError>
where
T: kube::Resource<Scope = NamespaceResourceScope>,
<T as kube::Resource>::DynamicType: Default,
T: Clone + DeserializeOwned + Debug + Serialize,
{
    info!("Creating {:?}", t.meta().name.as_ref().unwrap());
    let res_api: Api<T> = Api::namespaced(client.clone(), t.meta().namespace.as_ref().unwrap());
    let res = match res_api.create(&PostParams::default(), &t).await{
        Ok(res) => {
            Some(res)
        },
        Err(e) => {
            if is_not_found(&e){
                None
            } else {
                return Err(ReconcileError(e.into()));
            }
        },
    };
    Ok(res)
}

pub async fn update_status<T: kube::Resource>(t: &T, client: Client) -> Result<Option<T>, ReconcileError>
where
T: kube::Resource<Scope = NamespaceResourceScope>,
<T as kube::Resource>::DynamicType: Default,
T: Clone + DeserializeOwned + Debug + Serialize,
{
    info!("Updating Status {:?}", t.meta().name.as_ref().unwrap());
    let patch = serde_json::to_vec(&t).unwrap();
    let params = PostParams::default();
    let res_api: Api<T> = Api::namespaced(client.clone(), t.meta().namespace.as_ref().unwrap());
    let res = match res_api.replace_status(t.clone().meta().name.as_ref().unwrap(), &params, patch).await{
        Ok(res) => {
            Some(res)
        },
        Err(e) => {
            if is_not_found(&e){
                info!("status not found: {:?}", e);
                None
            } else {
                return Err(ReconcileError(e.into()));
            }
        },
    };
    Ok(res)
}
*/
