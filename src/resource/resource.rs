use std::collections::BTreeMap;
use futures::Future;
use futures::StreamExt;
use futures::TryFuture;
use k8s_openapi::NamespaceResourceScope;
use kube::core::ObjectMeta;
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::json;
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
    runtime::controller::{Action, Controller},
};
use serde_json::Value;
use kube::api::{Patch, PatchParams, ListParams, ObjectList};
use tracing::*;
use kube_runtime::finalizer::{self, finalizer, Event};
use crate::lxdmanager::lxdmanager::LxdClient;

#[derive(Debug)]
pub struct ReconcileError(pub anyhow::Error);
impl std::error::Error for ReconcileError {

}
impl std::fmt::Display for ReconcileError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f,"ReconcileError: {}", self.0)
    }
}

trait IsResult {
    type Ok;
    type Err;
}

impl<T, E> IsResult for Result<T, E> {
    type Ok = T;
    type Err = E;
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
    pub lxd_client: Option<LxdClient>,
}

impl<T> ResourceClient<T>
where
T: kube::Resource<Scope = NamespaceResourceScope> + serde::Serialize,
<T as kube::Resource>::DynamicType: Default,
T: Clone + DeserializeOwned + Debug,
{
    pub fn new(client: Client, lxd_client: Option<LxdClient>) -> Self {
        let api = Api::<T>::all(client.clone());
        Self {
            client,
            api,
            lxd_client,
        }
    }

    pub async fn list<R: kube::Resource>(&self, namespace: &str, labels: Option<BTreeMap<String, String>>) -> Result<Option<ObjectList<R>>, ReconcileError>
    where
    R: kube::Resource<Scope = NamespaceResourceScope>,
    <R as kube::Resource>::DynamicType: Default,
    R: Clone + DeserializeOwned + Debug,
    {
        let res_api: Api<R> = Api::namespaced(self.client.clone(), namespace);
        let mut list_params = ListParams::default();
        if let Some(labels) = labels{
            for (k, v) in labels.iter(){
                list_params.label_selector = Some(format!("{}={}", k, v));
            }
        }
        let res = match res_api.list(&list_params).await{
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

    pub async fn patch<R: kube::Resource>(&self, t: &R) -> Result<Option<R>, ReconcileError>
    where
    R: kube::Resource<Scope = NamespaceResourceScope>,
    <R as kube::Resource>::DynamicType: Default,
    R: Clone + DeserializeOwned + Debug + Serialize,
    {
        let patch = Patch::Merge(&t);
        let params = PatchParams::apply("virt.dev");
        let res_api: Api<R> = Api::namespaced(self.client.clone(), t.meta().namespace.as_ref().unwrap());
        let res = match res_api.patch(t.meta().name.as_ref().unwrap(), &params, &patch).await{
            Ok(res) => {
                Some(res)
            },
            Err(e) => {
                if is_not_found(&e){
                    None
                } else {
                    warn!("Error updating resource: {:?}", t);
                    return Err(ReconcileError(e.into()));
                }
            },
        };
        Ok(res)
    }

    pub async fn create_or_update<R: kube::Resource>(&self, t: &R) -> Result<Option<R>, ReconcileError>
    where
    R: kube::Resource<Scope = NamespaceResourceScope>,
    <R as kube::Resource>::DynamicType: Default,
    R: Clone + DeserializeOwned + Debug + Serialize,
    {
        match self.get::<R>(t.meta()).await{
            Ok(res) => {
                match res{
                    Some(t) => {                    
                        self.patch(&t).await
                    },
                    None => {
                        self.create(t).await
                    },
                }
            },
            Err(e) => {
                Err(e)
            },
        }
    }
    
    pub async fn update_status<R: kube::Resource>(&self, t: &R) -> Result<Option<R>, ReconcileError>
    where
        R: kube::Resource<Scope = NamespaceResourceScope>,
        <R as kube::Resource>::DynamicType: Default,
        R: Clone + DeserializeOwned + Debug + Serialize,
    {
        let patch = serde_json::to_vec(&t).unwrap();
        let params = PostParams::default();
        let res_api: Api<R> = Api::namespaced(self.client.clone(), t.meta().namespace.as_ref().unwrap());
        let res = match res_api.replace_status(t.clone().meta().name.as_ref().unwrap(), &params, patch).await{
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

    pub async fn setup_finalizer<R, ApplyFut, CleanupFut>(
        &self,
        obj: Arc<R>,
        resource_client: Arc<ResourceClient<R>>,
        mut apply: impl FnMut(Arc<R>, Arc<ResourceClient<R>>) -> ApplyFut,
        mut cleanup: impl FnMut(Arc<R>, Arc<ResourceClient<R>>) -> CleanupFut,
    ) -> Result<Action, ReconcileError>
    where
    <R as kube::Resource>::DynamicType: Default + Hash + Eq + Clone + std::fmt::Debug + Unpin,
    R: kube::Resource<Scope = NamespaceResourceScope>
        + for<'de> serde::Deserialize<'de>
        + Clone
        + Debug
        + Send
        + Sync
        + serde::Serialize
        + 'static,
        ApplyFut: Future<Output = Result<Action, ReconcileError>> + 'static + Send,
        CleanupFut: Future<Output = Result<Action, ReconcileError>> + 'static + Send,
    {
        let namespace = obj.meta().namespace.as_ref().unwrap();
        let api: Api<R> = Api::namespaced(resource_client.client.clone(), namespace);
        let res = finalizer(&api, "virt.dev/finalizer", obj, 
        |event| async {
            match event {
                Event::Apply(g) => {
                    apply(g, resource_client.clone()).await
                
                },
                Event::Cleanup(g) => {
                    cleanup(g, resource_client.clone()).await

                },
            }
        }
        ).await;

        match res{
            Ok(_) => {
                Ok(Action::requeue(std::time::Duration::from_secs(5 * 60)))
            },
            Err(e) => {
                match e{
                    finalizer::Error::RemoveFinalizer(e) => {
                        Err(ReconcileError(anyhow::anyhow!("RemoveFinalizer error: {:?}", e)))
                    },
                    finalizer::Error::AddFinalizer(e) => {
                        Err(ReconcileError(anyhow::anyhow!("AddFinalizer error: {:?}", e)))
                    },
                    finalizer::Error::ApplyFailed(e) => {
                        Err(ReconcileError(anyhow::anyhow!("ApplyFailed error: {:?}", e)))
                    },
                    finalizer::Error::CleanupFailed(e) => {
                        Err(ReconcileError(anyhow::anyhow!("CleanupFailed error: {:?}", e)))
                    },
                    finalizer::Error::UnnamedObject => {
                        Err(ReconcileError(anyhow::anyhow!("UnnamedObject error")))
                    },
                }
            }
        }
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
                    "Deleting {})",
                    o.name_any()
                );
            })
            .map_right(|_s| {
                // it's gone.
                info!("Deleted crd");
            })
        });
        // Wait for the delete to take place (map-left case or delete from previous run)
        sleep(Duration::from_secs(1)).await;
    
        // Create the CRD so we can create Foos in kube
        info!("Creating CRD");
        let pp = PostParams::default();
        //let patch_params = PatchParams::default();
        match crds.create(&pp, &crd).await {
            Ok(o) => {
                info!("Created {}", o.name_any());
            }
            Err(kube::Error::Api(ae)) => assert_eq!(ae.code, 409), // if you skipped delete, for instance
            Err(e) => return Err(e.into()),                        // any other case is probably bad
        }
        // Wait for the api to catch up
        sleep(Duration::from_secs(1)).await;
        Ok(())
    }

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
                    Ok(_o) => {},
                    Err(e) => warn!("reconcile failed: {:?}", e),
                }
            })
            .await;
        Ok(())
    }



}

pub fn is_not_found(e: &Error) -> bool {
    match e{
        kube::Error::Api(ae) => {
            match ae{
                kube::error::ErrorResponse { status: _, message: _, reason: r, code: _ } => {
                    match r.as_str(){
                        "NotFound" => {
                            return true
                        },
                        _ => {
                            return false
                        },
                    }
                },
            }
        },
        _ => {
            return false
        },
    }
}

pub async fn add_finalizer<T: kube::Resource>(api: Api<T>, name: &str) -> Result<T, ReconcileError>
where
    T: kube::Resource<Scope = NamespaceResourceScope>,
    <T as kube::Resource>::DynamicType: Default,
    T: Clone + DeserializeOwned + Debug + Serialize,
{
    let finalizer: Value = json!({
        "metadata": {
            "finalizers": ["virt.dev/finalizer"]
        }
    });

    let patch: Patch<&Value> = Patch::Merge(&finalizer);
    match api.patch(name, &PatchParams::default(), &patch).await{
       Ok(res) => {
           Ok(res)
       },
       Err(e) => {
        return Err(ReconcileError(e.into()))
       }
    }
}

pub async fn del_finalizer<T: kube::Resource>(api: Api<T>, name: &str) -> Result<T, ReconcileError>
where
    T: kube::Resource<Scope = NamespaceResourceScope>,
    <T as kube::Resource>::DynamicType: Default,
    T: Clone + DeserializeOwned + Debug + Serialize,
{
    let finalizer: Value = json!({
        "metadata": {
            "finalizers": null
        }
    });

    let patch: Patch<&Value> = Patch::Merge(&finalizer);
    match api.patch(name, &PatchParams::default(), &patch).await{
        Ok(res) => {
            Ok(res)
        },
        Err(e) => {
         return Err(ReconcileError(e.into()))
        }
     }
}

pub enum ReconcileAction {
    Create,
    Delete,
    NoOp,
}

pub fn reconcile_action<T: kube::Resource>(t: &T) -> ReconcileAction 
where
    T: kube::Resource<Scope = NamespaceResourceScope>,
    <T as kube::Resource>::DynamicType: Default,
    T: Clone + DeserializeOwned + Debug + Serialize,
{
    return if t.meta().deletion_timestamp.is_some() {
        ReconcileAction::Delete
    } else if t.meta().finalizers.is_none() {
        ReconcileAction::Create
    } else {
        ReconcileAction::NoOp
    };
}
