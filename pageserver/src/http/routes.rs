use std::sync::Arc;

use anyhow::{Context, Result};
use hyper::header;
use hyper::StatusCode;
use hyper::{Body, Request, Response, Uri};
use serde::Serialize;
use tracing::*;
use zenith_utils::auth::JwtAuth;
use zenith_utils::http::endpoint::attach_openapi_ui;
use zenith_utils::http::endpoint::auth_middleware;
use zenith_utils::http::endpoint::check_permission;
use zenith_utils::http::error::ApiError;
use zenith_utils::http::{
    endpoint,
    error::HttpErrorBody,
    json::{json_request, json_response},
    request::get_request_param,
    request::parse_request_param,
};
use zenith_utils::http::{RequestExt, RouterBuilder};
use zenith_utils::lsn::Lsn;
use zenith_utils::zid::{opt_display_serde, ZTimelineId};

use super::models::BranchCreateRequest;
use super::models::TenantCreateRequest;
use crate::branches::BranchInfo;
use crate::remote_storage::schedule_timeline_download;
use crate::remote_storage::RemoteTimelineIndex;
use crate::repository::LocalTimelineState;
use crate::repository::RepositoryTimeline;
use crate::{branches, config::PageServerConf, tenant_mgr, ZTenantId};
use tokio::sync::RwLock;
use zenith_utils::zid::ZTenantTimelineId;

#[derive(Debug)]
struct State {
    conf: &'static PageServerConf,
    auth: Option<Arc<JwtAuth>>,
    remote_index: Arc<RwLock<RemoteTimelineIndex>>,
    allowlist_routes: Vec<Uri>,
}

impl State {
    fn new(
        conf: &'static PageServerConf,
        auth: Option<Arc<JwtAuth>>,
        remote_index: Arc<RwLock<RemoteTimelineIndex>>,
    ) -> Self {
        let allowlist_routes = ["/v1/status", "/v1/doc", "/swagger.yml"]
            .iter()
            .map(|v| v.parse().unwrap())
            .collect::<Vec<_>>();
        Self {
            conf,
            auth,
            allowlist_routes,
            remote_index,
        }
    }
}

#[inline(always)]
fn get_state(request: &Request<Body>) -> &State {
    request
        .data::<Arc<State>>()
        .expect("unknown state type")
        .as_ref()
}

#[inline(always)]
fn get_config(request: &Request<Body>) -> &'static PageServerConf {
    get_state(request).conf
}

// healthcheck handler
async fn status_handler(_: Request<Body>) -> Result<Response<Body>, ApiError> {
    Ok(Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from("{}"))
        .map_err(ApiError::from_err)?)
}

async fn branch_create_handler(mut request: Request<Body>) -> Result<Response<Body>, ApiError> {
    let request_data: BranchCreateRequest = json_request(&mut request).await?;

    check_permission(&request, Some(request_data.tenant_id))?;

    let response_data = tokio::task::spawn_blocking(move || {
        let _enter = info_span!("/branch_create", name = %request_data.name, tenant = %request_data.tenant_id, startpoint=%request_data.start_point).entered();
        branches::create_branch(
            get_config(&request),
            &request_data.name,
            &request_data.start_point,
            &request_data.tenant_id,
        )
    })
    .await
    .map_err(ApiError::from_err)??;
    Ok(json_response(StatusCode::CREATED, response_data)?)
}

// Gate non incremental logical size calculation behind a flag
// after pgbench -i -s100 calculation took 28ms so if multiplied by the number of timelines
// and tenants it can take noticeable amount of time. Also the value currently used only in tests
fn get_include_non_incremental_logical_size(request: &Request<Body>) -> bool {
    request
        .uri()
        .query()
        .map(|v| {
            url::form_urlencoded::parse(v.as_bytes())
                .into_owned()
                .any(|(param, _)| param == "include-non-incremental-logical-size")
        })
        .unwrap_or(false)
}

async fn branch_list_handler(request: Request<Body>) -> Result<Response<Body>, ApiError> {
    let tenantid: ZTenantId = parse_request_param(&request, "tenant_id")?;

    let include_non_incremental_logical_size = get_include_non_incremental_logical_size(&request);

    check_permission(&request, Some(tenantid))?;

    let response_data = tokio::task::spawn_blocking(move || {
        let _enter = info_span!("branch_list", tenant = %tenantid).entered();
        crate::branches::get_branches(
            get_config(&request),
            &tenantid,
            include_non_incremental_logical_size,
        )
    })
    .await
    .map_err(ApiError::from_err)??;
    Ok(json_response(StatusCode::OK, response_data)?)
}

async fn branch_detail_handler(request: Request<Body>) -> Result<Response<Body>, ApiError> {
    let tenantid: ZTenantId = parse_request_param(&request, "tenant_id")?;
    let branch_name: String = get_request_param(&request, "branch_name")?.to_string();
    let conf = get_state(&request).conf;
    let path = conf.branch_path(&branch_name, &tenantid);

    let include_non_incremental_logical_size = get_include_non_incremental_logical_size(&request);

    let response_data = tokio::task::spawn_blocking(move || {
        let _enter = info_span!("branch_detail", tenant = %tenantid, branch=%branch_name).entered();
        let repo = tenant_mgr::get_repository_for_tenant(tenantid)?;
        BranchInfo::from_path(path, &repo, include_non_incremental_logical_size)
    })
    .await
    .map_err(ApiError::from_err)??;

    Ok(json_response(StatusCode::OK, response_data)?)
}

async fn timeline_list_handler(request: Request<Body>) -> Result<Response<Body>, ApiError> {
    let tenant_id: ZTenantId = parse_request_param(&request, "tenant_id")?;
    check_permission(&request, Some(tenant_id))?;

    let conf = get_state(&request).conf;
    let timelines_dir = conf.timelines_path(&tenant_id);

    let mut timelines_dir_contents =
        tokio::fs::read_dir(&timelines_dir).await.with_context(|| {
            format!(
                "Failed to list timelines dir '{}' contents",
                timelines_dir.display()
            )
        })?;

    let mut local_timelines = Vec::new();
    while let Some(entry) = timelines_dir_contents.next_entry().await.with_context(|| {
        format!(
            "Failed to list timelines dir '{}' contents",
            timelines_dir.display()
        )
    })? {
        let entry_path = entry.path();
        let entry_type = entry.file_type().await.with_context(|| {
            format!(
                "Failed to get file type of timeline dirs' entry '{}'",
                entry_path.display()
            )
        })?;

        if entry_type.is_dir() {
            match entry.file_name().to_string_lossy().parse::<ZTimelineId>() {
                Ok(timeline_id) => local_timelines.push(timeline_id.to_string()),
                Err(e) => error!(
                    "Failed to get parse timeline id from timeline dirs' entry '{}': {}",
                    entry_path.display(),
                    e
                ),
            }
        }
    }

    Ok(json_response(StatusCode::OK, local_timelines)?)
}

#[derive(Debug, Serialize)]
struct LocalTimelineInfo {
    #[serde(with = "opt_display_serde")]
    ancestor_timeline_id: Option<ZTimelineId>,
    last_record_lsn: Lsn,
    prev_record_lsn: Option<Lsn>,
    disk_consistent_lsn: Lsn,
    timeline_state: LocalTimelineState,
}

#[derive(Debug, Serialize)]
struct RemoteTimelineInfo {
    remote_consistent_lsn: Option<Lsn>,
    awaits_download: bool,
}

#[derive(Debug, Serialize)]
#[serde(tag = "type")]
struct TimelineInfo {
    #[serde(with = "hex")]
    timeline_id: ZTimelineId,
    #[serde(with = "hex")]
    tenant_id: ZTenantId,
    local: Option<LocalTimelineInfo>,
    remote: Option<RemoteTimelineInfo>,
}

async fn timeline_detail_handler(request: Request<Body>) -> Result<Response<Body>, ApiError> {
    let tenant_id: ZTenantId = parse_request_param(&request, "tenant_id")?;
    check_permission(&request, Some(tenant_id))?;

    let timeline_id: ZTimelineId = parse_request_param(&request, "timeline_id")?;

    let span = info_span!("timeline_detail_handler", tenant = %tenant_id, timeline = %timeline_id);

    let (local_timeline_info, span) = tokio::task::spawn_blocking(move || {
        let entered = span.entered();
        let repo = tenant_mgr::get_repository_for_tenant(tenant_id)?;
        let local_timeline = {
            repo.get_timeline(timeline_id)
                .map(|timeline| match timeline {
                    RepositoryTimeline::Loaded(loaded_timeline) => LocalTimelineInfo {
                        ancestor_timeline_id: loaded_timeline.get_ancestor_timeline_id(),
                        disk_consistent_lsn: loaded_timeline.get_disk_consistent_lsn(),
                        last_record_lsn: loaded_timeline.get_last_record_lsn(),
                        prev_record_lsn: Some(loaded_timeline.get_prev_record_lsn()),
                        timeline_state: LocalTimelineState::Loaded(loaded_timeline.get_state()),
                    },
                    RepositoryTimeline::Unloaded { metadata } => LocalTimelineInfo {
                        ancestor_timeline_id: metadata.ancestor_timeline(),
                        disk_consistent_lsn: metadata.disk_consistent_lsn(),
                        last_record_lsn: metadata.disk_consistent_lsn(),
                        prev_record_lsn: metadata.prev_record_lsn(),
                        timeline_state: LocalTimelineState::Unloaded,
                    },
                })
        };
        Ok::<_, anyhow::Error>((local_timeline, entered.exit()))
    })
    .await
    .map_err(ApiError::from_err)??;

    let remote_timeline_info = {
        let remote_index_read = get_state(&request).remote_index.read().await;
        remote_index_read
            .timeline_entry(&ZTenantTimelineId {
                tenant_id,
                timeline_id,
            })
            .map(|remote_entry| RemoteTimelineInfo {
                remote_consistent_lsn: remote_entry.disk_consistent_lsn(),
                awaits_download: remote_entry.get_awaits_download(),
            })
    };

    let _enter = span.entered();

    if local_timeline_info.is_none() && remote_timeline_info.is_none() {
        return Err(ApiError::NotFound(
            "Timeline is not found neither locally nor remotely".to_string(),
        ));
    }

    Ok(json_response(
        StatusCode::OK,
        TimelineInfo {
            tenant_id,
            timeline_id,
            local: local_timeline_info,
            remote: remote_timeline_info,
        },
    )?)
}

async fn timeline_attach_handler(request: Request<Body>) -> Result<Response<Body>, ApiError> {
    let tenant_id: ZTenantId = parse_request_param(&request, "tenant_id")?;
    check_permission(&request, Some(tenant_id))?;

    let timeline_id: ZTimelineId = parse_request_param(&request, "timeline_id")?;
    let span = info_span!("timeline_attach_handler", tenant = %tenant_id, timeline = %timeline_id);

    let span = tokio::task::spawn_blocking(move || {
        let entered = span.entered();
        if tenant_mgr::get_timeline_for_tenant_load(tenant_id, timeline_id).is_ok() {
            anyhow::bail!("Timeline is already present locally")
        };
        Ok(entered.exit())
    })
    .await
    .map_err(ApiError::from_err)??;

    let mut remote_index_write = get_state(&request).remote_index.write().await;

    let _enter = span.entered(); // entered guard cannot live across awaits (non Send)
    let index_entry = remote_index_write
        .timeline_entry_mut(&ZTenantTimelineId {
            tenant_id,
            timeline_id,
        })
        .ok_or_else(|| ApiError::BadRequest("Unknown remote timeline".to_string()))?;

    if index_entry.get_awaits_download() {
        return Err(ApiError::NotFound(
            "Timeline download is already in progress".to_string(),
        ));
    }

    index_entry.set_awaits_download(true);
    schedule_timeline_download(tenant_id, timeline_id);

    Ok(json_response(StatusCode::ACCEPTED, ())?)
}

async fn timeline_detach_handler(request: Request<Body>) -> Result<Response<Body>, ApiError> {
    let tenant_id: ZTenantId = parse_request_param(&request, "tenant_id")?;
    check_permission(&request, Some(tenant_id))?;

    let timeline_id: ZTimelineId = parse_request_param(&request, "timeline_id")?;

    tokio::task::spawn_blocking(move || {
        let _enter =
            info_span!("timeline_detach_handler", tenant = %tenant_id, timeline = %timeline_id)
                .entered();
        let repo = tenant_mgr::get_repository_for_tenant(tenant_id)?;
        repo.detach_timeline(timeline_id)
    })
    .await
    .map_err(ApiError::from_err)??;

    Ok(json_response(StatusCode::OK, ())?)
}

async fn tenant_list_handler(request: Request<Body>) -> Result<Response<Body>, ApiError> {
    // check for management permission
    check_permission(&request, None)?;

    let response_data = tokio::task::spawn_blocking(move || {
        let _enter = info_span!("tenant_list").entered();
        crate::tenant_mgr::list_tenants()
    })
    .await
    .map_err(ApiError::from_err)??;

    Ok(json_response(StatusCode::OK, response_data)?)
}

async fn tenant_create_handler(mut request: Request<Body>) -> Result<Response<Body>, ApiError> {
    // check for management permission
    check_permission(&request, None)?;

    let request_data: TenantCreateRequest = json_request(&mut request).await?;
    let remote_index = Arc::clone(&get_state(&request).remote_index);

    tokio::task::spawn_blocking(move || {
        let _enter = info_span!("tenant_create", tenant = %request_data.tenant_id).entered();
        tenant_mgr::create_repository_for_tenant(
            get_config(&request),
            request_data.tenant_id,
            remote_index,
        )
    })
    .await
    .map_err(ApiError::from_err)??;
    Ok(json_response(StatusCode::CREATED, ())?)
}

async fn handler_404(_: Request<Body>) -> Result<Response<Body>, ApiError> {
    json_response(
        StatusCode::NOT_FOUND,
        HttpErrorBody::from_msg("page not found".to_owned()),
    )
}

pub fn make_router(
    conf: &'static PageServerConf,
    auth: Option<Arc<JwtAuth>>,
    remote_index: Arc<RwLock<RemoteTimelineIndex>>,
) -> RouterBuilder<hyper::Body, ApiError> {
    let spec = include_bytes!("openapi_spec.yml");
    let mut router = attach_openapi_ui(endpoint::make_router(), spec, "/swagger.yml", "/v1/doc");
    if auth.is_some() {
        router = router.middleware(auth_middleware(|request| {
            let state = get_state(request);
            if state.allowlist_routes.contains(request.uri()) {
                None
            } else {
                state.auth.as_deref()
            }
        }))
    }

    router
        .data(Arc::new(State::new(conf, auth, remote_index)))
        .get("/v1/status", status_handler)
        .get("/v1/timeline/:tenant_id", timeline_list_handler)
        .get(
            "/v1/timeline/:tenant_id/:timeline_id",
            timeline_detail_handler,
        )
        .post(
            "/v1/timeline/:tenant_id/:timeline_id/attach",
            timeline_attach_handler,
        )
        .post(
            "/v1/timeline/:tenant_id/:timeline_id/detach",
            timeline_detach_handler,
        )
        .get("/v1/branch/:tenant_id", branch_list_handler)
        .get("/v1/branch/:tenant_id/:branch_name", branch_detail_handler)
        .post("/v1/branch", branch_create_handler)
        .get("/v1/tenant", tenant_list_handler)
        .post("/v1/tenant", tenant_create_handler)
        .any(handler_404)
}
