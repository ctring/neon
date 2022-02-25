use serde::{Deserialize, Serialize};

use crate::ZTenantId;

#[derive(Serialize, Deserialize)]
pub struct BranchCreateRequest {
    #[serde(with = "hex")]
    pub tenant_id: ZTenantId,
    pub name: String,
    pub start_point: String,
}

#[derive(Serialize, Deserialize)]
pub struct TenantCreateRequest {
    #[serde(with = "hex")]
    pub tenant_id: ZTenantId,
    pub checkpoint_distance: Option<u64>,
    pub checkpoint_period: Option<String>,
    pub gc_horizon: Option<u64>,
    pub gc_period: Option<String>,
    pub pitr_interval: Option<String>,
}

impl TenantCreateRequest {
    pub fn new(tenant_id: ZTenantId) -> TenantCreateRequest {
        TenantCreateRequest {
            tenant_id,
            checkpoint_distance: None,
            checkpoint_period: None,
            gc_horizon: None,
            gc_period: None,
            pitr_interval: None,
        }
    }
}
