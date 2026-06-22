use std::{
    collections::HashMap,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    path::{Path, PathBuf},
    sync::Arc,
};

use axum::{
    Json, Router,
    extract::{Query, State},
    http::{StatusCode, header},
    response::IntoResponse,
    routing::{get, post},
};
use clap::Parser;
use color_eyre::eyre::Result as EyreResult;
use rusqlite::{
    Connection, OpenFlags, params_from_iter,
    types::{Value as SqlValue, ValueRef},
};
use serde::{Deserialize, Serialize};
use serde_json::Value;

const INDEX_HTML: &[u8] = include_bytes!("../assets/html/index.html");
const LOGS_HTML: &[u8] = include_bytes!("../assets/html/logs.html");
const QUERY_HTML: &[u8] = include_bytes!("../assets/html/query.html");
const BASE_CSS: &[u8] = include_bytes!("../assets/css/base.css");
const COMMON_JS: &[u8] = include_bytes!("../assets/js/common.js");
const LIVE_JS: &[u8] = include_bytes!("../assets/js/live.js");
const LOGS_JS: &[u8] = include_bytes!("../assets/js/logs.js");
const QUERY_JS: &[u8] = include_bytes!("../assets/js/query.js");
const DEFAULT_LOG_LIMIT: usize = 50;
const MAX_LOG_LIMIT: usize = 500;

type ApiError = (StatusCode, Json<QueryError>);
type ApiResult<T> = std::result::Result<Json<T>, ApiError>;
type SqlResult<T> = std::result::Result<T, String>;

#[derive(Clone)]
struct AppState {
    database_path: Arc<PathBuf>,
}

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Cli {
    /// The port that the website is served on.
    listening_port: u16,
    /// The filepath where the spider log database is located.
    database_path: PathBuf,
}

#[tokio::main]
async fn main() -> EyreResult<()> {
    color_eyre::install()?;
    let cli = Cli::parse();

    open_readonly_database(&cli.database_path)?;
    let state = AppState {
        database_path: Arc::new(cli.database_path),
    };

    let app = Router::new()
        .route("/", get(index_html))
        .route("/index.html", get(index_html))
        .route("/logs", get(logs_html))
        .route("/logs.html", get(logs_html))
        .route("/query", get(query_html))
        .route("/query.html", get(query_html))
        .route("/assets/css/base.css", get(base_css))
        .route("/assets/js/common.js", get(common_js))
        .route("/assets/js/live.js", get(live_js))
        .route("/assets/js/logs.js", get(logs_js))
        .route("/assets/js/query.js", get(query_js))
        .route("/api/logs", get(list_logs))
        .route("/api/log-filters", get(log_filters))
        .route("/api/query", post(query_sql))
        .with_state(state);

    let listen_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)), cli.listening_port);
    let listener = tokio::net::TcpListener::bind(listen_addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}

async fn index_html() -> impl IntoResponse {
    html(INDEX_HTML)
}

async fn query_html() -> impl IntoResponse {
    html(QUERY_HTML)
}

async fn logs_html() -> impl IntoResponse {
    html(LOGS_HTML)
}

async fn base_css() -> impl IntoResponse {
    asset("text/css; charset=utf-8", BASE_CSS)
}

async fn common_js() -> impl IntoResponse {
    javascript(COMMON_JS)
}

async fn live_js() -> impl IntoResponse {
    javascript(LIVE_JS)
}

async fn logs_js() -> impl IntoResponse {
    javascript(LOGS_JS)
}

async fn query_js() -> impl IntoResponse {
    javascript(QUERY_JS)
}

fn html(body: &'static [u8]) -> impl IntoResponse {
    asset("text/html; charset=utf-8", body)
}

fn javascript(body: &'static [u8]) -> impl IntoResponse {
    asset("text/javascript; charset=utf-8", body)
}

fn asset(content_type: &'static str, body: &'static [u8]) -> impl IntoResponse {
    ([(header::CONTENT_TYPE, content_type)], body)
}

#[derive(Deserialize)]
struct QueryRequest {
    sql: String,
    #[serde(default)]
    replace_names: bool,
}

#[derive(Serialize, Debug, PartialEq)]
struct QueryResponse {
    columns: Vec<String>,
    rows: Vec<Vec<Value>>,
    row_count: usize,
}

#[derive(Serialize)]
struct QueryError {
    error: String,
}

#[derive(Deserialize, Clone, Debug, Default)]
struct LogsRequest {
    limit: Option<usize>,
    level: Option<i64>,
    producer: Option<i64>,
    utc_lowest: Option<i64>,
    utc_highest: Option<i64>,
    content: Option<String>,
}

#[derive(Serialize, Debug, PartialEq)]
struct LogsResponse {
    rows: Vec<LogRow>,
    row_count: usize,
    limit: usize,
}

#[derive(Serialize, Debug, PartialEq)]
struct LogRow {
    id: i64,
    occurrence: i64,
    level_id: i64,
    level: String,
    producer_id: i64,
    producer: String,
    content: String,
}

#[derive(Serialize, Debug, PartialEq)]
struct LogFiltersResponse {
    levels: Vec<LevelFilter>,
    producers: Vec<ProducerFilter>,
}

#[derive(Serialize, Debug, PartialEq)]
struct LevelFilter {
    id: i64,
    text: String,
}

#[derive(Serialize, Debug, PartialEq)]
struct ProducerFilter {
    id: i64,
    name: String,
}

enum DatabaseTaskError {
    Query(String),
    Internal(String),
}

async fn list_logs(
    State(state): State<AppState>,
    Query(request): Query<LogsRequest>,
) -> ApiResult<LogsResponse> {
    with_database(state, "log loading", move |connection| {
        load_logs(connection, request)
    })
    .await
}

async fn log_filters(State(state): State<AppState>) -> ApiResult<LogFiltersResponse> {
    with_database(state, "filter loading", load_log_filters).await
}

async fn query_sql(
    State(state): State<AppState>,
    Json(request): Json<QueryRequest>,
) -> ApiResult<QueryResponse> {
    with_database(state, "query", move |connection| {
        execute_query(connection, &request.sql, request.replace_names)
    })
    .await
}

async fn with_database<T, F>(state: AppState, task_name: &'static str, operation: F) -> ApiResult<T>
where
    T: Send + 'static,
    F: FnOnce(&Connection) -> SqlResult<T> + Send + 'static,
{
    let database_path = state.database_path.clone();
    let result = tokio::task::spawn_blocking(move || {
        let connection = open_readonly_database(&*database_path)
            .map_err(|err| DatabaseTaskError::Internal(format!("database open failed: {err}")))?;

        operation(&connection).map_err(DatabaseTaskError::Query)
    })
    .await
    .map_err(|err| {
        query_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("{task_name} task failed: {err}"),
        )
    })?;

    match result {
        Ok(response) => Ok(Json(response)),
        Err(DatabaseTaskError::Query(err)) => Err(query_error(StatusCode::BAD_REQUEST, err)),
        Err(DatabaseTaskError::Internal(err)) => {
            Err(query_error(StatusCode::INTERNAL_SERVER_ERROR, err))
        }
    }
}

fn query_error(status: StatusCode, error: String) -> ApiError {
    (status, Json(QueryError { error }))
}

fn open_readonly_database<P: AsRef<Path>>(path: P) -> EyreResult<Connection> {
    let connection = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY)?;
    connection.execute_batch("PRAGMA query_only = ON;")?;
    Ok(connection)
}

fn load_logs(connection: &Connection, request: LogsRequest) -> SqlResult<LogsResponse> {
    let limit = validate_logs_request(connection, &request)?;
    let content_filter = request
        .content
        .as_deref()
        .map(str::trim)
        .filter(|content| !content.is_empty())
        .map(str::to_lowercase);

    let mut sql = String::from(
        r#"
        SELECT
            Log.id,
            Log.occurrence,
            Log.level,
            Level.text,
            Log.producer,
            Producer.name,
            Log.content
        FROM Log
        JOIN Level ON Level.id = Log.level
        JOIN Producer ON Producer.id = Log.producer
        WHERE 1 = 1
        "#,
    );
    let mut values = Vec::new();

    if let Some(level) = request.level {
        sql.push_str(" AND Log.level = ?");
        values.push(SqlValue::Integer(level));
    }

    if let Some(producer) = request.producer {
        sql.push_str(" AND Log.producer = ?");
        values.push(SqlValue::Integer(producer));
    }

    if let Some(utc_lowest) = request.utc_lowest {
        sql.push_str(" AND Log.occurrence >= ?");
        values.push(SqlValue::Integer(utc_lowest));
    }

    if let Some(utc_highest) = request.utc_highest {
        sql.push_str(" AND Log.occurrence <= ?");
        values.push(SqlValue::Integer(utc_highest));
    }

    if let Some(content) = content_filter {
        sql.push_str(" AND instr(lower(Log.content), ?) > 0");
        values.push(SqlValue::Text(content));
    }

    sql.push_str(" ORDER BY Log.id DESC LIMIT ?");
    values.push(SqlValue::Integer(limit as i64));

    let mut statement = connection.prepare(&sql).map_err(sql_error)?;
    let rows = statement
        .query_map(params_from_iter(values.iter()), |row| {
            Ok(LogRow {
                id: row.get(0)?,
                occurrence: row.get(1)?,
                level_id: row.get(2)?,
                level: row.get(3)?,
                producer_id: row.get(4)?,
                producer: row.get(5)?,
                content: row.get(6)?,
            })
        })
        .map_err(sql_error)?
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(sql_error)?;

    Ok(LogsResponse {
        row_count: rows.len(),
        rows,
        limit,
    })
}

fn validate_logs_request(connection: &Connection, request: &LogsRequest) -> SqlResult<usize> {
    let limit = request.limit.unwrap_or(DEFAULT_LOG_LIMIT);

    if limit == 0 || limit > MAX_LOG_LIMIT {
        return Err(format!(
            "limit must be between 1 and {MAX_LOG_LIMIT}, got {limit}"
        ));
    }

    if let Some(level) = request.level {
        ensure_id_exists(
            connection,
            "SELECT EXISTS(SELECT 1 FROM Level WHERE id = ?1)",
            "Level",
            level,
        )?;
    }

    if let Some(producer) = request.producer {
        ensure_id_exists(
            connection,
            "SELECT EXISTS(SELECT 1 FROM Producer WHERE id = ?1)",
            "Producer",
            producer,
        )?;
    }

    match request.utc_lowest {
        Some(utc_lowest) if utc_lowest < 0 => {
            return Err(format!(
                "utc_lowest must be greater than or equal to 0, got {utc_lowest}"
            ));
        }
        _ => {}
    }

    match request.utc_highest {
        Some(utc_highest) if utc_highest < 0 => {
            return Err(format!(
                "utc_highest must be greater than or equal to 0, got {utc_highest}"
            ));
        }
        _ => {}
    }

    match (request.utc_lowest, request.utc_highest) {
        (Some(utc_lowest), Some(utc_highest)) if utc_lowest > utc_highest => {
            return Err(format!(
                "utc_lowest must be less than or equal to utc_highest, got {utc_lowest} > {utc_highest}"
            ));
        }
        _ => {}
    }

    Ok(limit)
}

fn ensure_id_exists(connection: &Connection, sql: &str, label: &str, id: i64) -> SqlResult<()> {
    let exists: i64 = connection
        .query_row(sql, [id], |row| row.get(0))
        .map_err(sql_error)?;

    if exists == 0 {
        return Err(format!("{label} id {id} does not exist"));
    }

    Ok(())
}

fn load_log_filters(connection: &Connection) -> SqlResult<LogFiltersResponse> {
    let mut level_statement = connection
        .prepare("SELECT id, text FROM Level ORDER BY id")
        .map_err(sql_error)?;
    let levels = level_statement
        .query_map([], |row| {
            Ok(LevelFilter {
                id: row.get(0)?,
                text: row.get(1)?,
            })
        })
        .map_err(sql_error)?
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(sql_error)?;

    let mut producer_statement = connection
        .prepare("SELECT id, name FROM Producer ORDER BY name COLLATE NOCASE, id")
        .map_err(sql_error)?;
    let producers = producer_statement
        .query_map([], |row| {
            Ok(ProducerFilter {
                id: row.get(0)?,
                name: row.get(1)?,
            })
        })
        .map_err(sql_error)?
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(sql_error)?;

    Ok(LogFiltersResponse { levels, producers })
}

fn execute_query(
    connection: &Connection,
    sql: &str,
    replace_names: bool,
) -> SqlResult<QueryResponse> {
    let sql = sql.trim();
    if sql.is_empty() {
        return Err("SQL query cannot be empty".to_owned());
    }

    let (columns, mut result_rows) = {
        let mut statement = connection.prepare(sql).map_err(sql_error)?;
        let columns = statement
            .column_names()
            .into_iter()
            .map(str::to_owned)
            .collect::<Vec<_>>();
        let column_count = statement.column_count();
        let mut rows = statement.query([]).map_err(sql_error)?;
        let mut result_rows = Vec::new();

        while let Some(row) = rows.next().map_err(sql_error)? {
            let mut values = Vec::with_capacity(column_count);

            for column_index in 0..column_count {
                values.push(sqlite_value_to_json(
                    row.get_ref(column_index).map_err(sql_error)?,
                ));
            }

            result_rows.push(values);
        }

        (columns, result_rows)
    };

    if replace_names {
        replace_producer_and_level_ids(connection, &columns, &mut result_rows)?;
    }

    Ok(QueryResponse {
        columns,
        row_count: result_rows.len(),
        rows: result_rows,
    })
}

fn replace_producer_and_level_ids(
    connection: &Connection,
    columns: &[String],
    rows: &mut [Vec<Value>],
) -> SqlResult<()> {
    let producer_columns = matching_columns(columns, "producer");
    let level_columns = matching_columns(columns, "level");

    if producer_columns.is_empty() && level_columns.is_empty() {
        return Ok(());
    }

    let producer_names = if producer_columns.is_empty() {
        HashMap::new()
    } else {
        load_id_name_map(connection, "SELECT id, name FROM Producer")?
    };
    let level_names = if level_columns.is_empty() {
        HashMap::new()
    } else {
        load_id_name_map(connection, "SELECT id, text FROM Level")?
    };

    replace_ids(rows, &producer_columns, &producer_names);
    replace_ids(rows, &level_columns, &level_names);
    Ok(())
}

fn matching_columns(columns: &[String], name: &str) -> Vec<usize> {
    columns
        .iter()
        .enumerate()
        .filter_map(|(index, column)| column.eq_ignore_ascii_case(name).then_some(index))
        .collect()
}

fn load_id_name_map(connection: &Connection, sql: &str) -> SqlResult<HashMap<i64, String>> {
    let mut statement = connection.prepare(sql).map_err(sql_error)?;
    statement
        .query_map([], |row| {
            Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
        })
        .map_err(sql_error)?
        .collect::<std::result::Result<HashMap<_, _>, _>>()
        .map_err(sql_error)
}

fn replace_ids(rows: &mut [Vec<Value>], columns: &[usize], names: &HashMap<i64, String>) {
    for row in rows {
        for &index in columns {
            if let Some(value) = row.get_mut(index) {
                replace_id(value, names);
            }
        }
    }
}

fn replace_id(value: &mut Value, names: &HashMap<i64, String>) {
    let Some(id) = value.as_i64() else {
        return;
    };

    let Some(name) = names.get(&id) else {
        return;
    };

    *value = Value::String(name.clone());
}

fn sqlite_value_to_json(value: ValueRef<'_>) -> Value {
    match value {
        ValueRef::Null => Value::Null,
        ValueRef::Integer(value) => Value::from(value),
        ValueRef::Real(value) => serde_json::Number::from_f64(value)
            .map(Value::Number)
            .unwrap_or_else(|| Value::String(value.to_string())),
        ValueRef::Text(value) => Value::String(String::from_utf8_lossy(value).into_owned()),
        ValueRef::Blob(value) => Value::String(format!("<blob {} bytes>", value.len())),
    }
}

fn sql_error(error: rusqlite::Error) -> String {
    error.to_string()
}
