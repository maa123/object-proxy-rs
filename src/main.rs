use std::collections::HashMap;
use std::sync::Arc;

use actix_web::{get, web, App, HttpRequest, HttpResponse, HttpServer, Responder};

use rusoto_core::{credential::StaticProvider, Region};
use rusoto_s3::{GetObjectRequest, S3Client, S3};

use config::Value;

use tokio::io;

struct Storage {
    region: Region,
    credential: Credential,
    bucket: String,
}

struct Credential {
    key: String,
    secret: String,
}

struct Bucket {
    client: S3Client,
    name: String,
}

struct AppBucketList {
    buckets: Vec<Bucket>,
}

#[get("/")]
async fn ok() -> impl Responder {
    HttpResponse::Ok().body("OK")
}

async fn get_object(bucket: &Bucket, key: &str) -> Option<Vec<u8>> {
    let mut req = GetObjectRequest::default();
    req.bucket = bucket.name.to_string();
    req.key = key.to_string();
    req.key.remove(0);
    if let Ok(mut obj) = bucket.client.get_object(req).await {
        if let Some(streaming) = obj.body.take() {
            let mut buf: Vec<u8> = vec![];
            let mut body = streaming.into_async_read();
            io::copy(&mut body, &mut buf).await.unwrap();
            return Some(buf);
        }
    }
    None
}

async fn object_req(req: HttpRequest, data: web::Data<Arc<AppBucketList>>) -> impl Responder {
    let bucket_state = data;
    let path = req.uri().path();
    for bucket in &bucket_state.buckets {
        if let Some(buf) = get_object(bucket, path).await {
            return HttpResponse::Ok().body(buf);
        }
    }
    HttpResponse::NotFound()
        .content_type("text/plain")
        .body("Not Found")
}

fn parse_bucket_config_credentials(cfg: &HashMap<String, Value>) -> Credential {
    Credential {
        key: cfg
            .get("access-key")
            .unwrap_or(&Value::from(""))
            .to_string(),
        secret: cfg
            .get("secret-key")
            .unwrap_or(&Value::from(""))
            .to_string(),
    }
}

fn parse_bucket_config_region(cfg: &HashMap<String, Value>) -> Region {
    if cfg.contains_key("endpoint") {
        return Region::Custom {
            name: cfg
                .get("region")
                .unwrap_or(&Value::from("us-east-1"))
                .to_string(),
            endpoint: cfg.get("endpoint").unwrap_or(&Value::from("")).to_string(),
        };
    }
    cfg.get("region")
        .unwrap_or(&Value::from("us-east-1"))
        .to_string()
        .parse()
        .unwrap_or(Region::UsEast1)
}

fn parse_bucket_config(cfg: HashMap<String, Value>) -> Storage {
    Storage {
        region: parse_bucket_config_region(&cfg),
        credential: parse_bucket_config_credentials(&cfg),
        bucket: cfg
            .get("bucket")
            .unwrap_or(&Value::from("bucket"))
            .to_string(),
    }
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let mut settings = config::Config::default();
    settings.merge(config::File::with_name("config")).unwrap();
    let config_bucket_list = settings.get_array("bucket").unwrap_or(Vec::new());
    let mut bucket_list: Vec<Bucket> = Vec::with_capacity(config_bucket_list.capacity());
    for bucket in config_bucket_list {
        let t = bucket.into_table().unwrap();
        let storage = parse_bucket_config(t);
        let provider = StaticProvider::new_minimal(
            storage.credential.key.to_string(),
            storage.credential.secret.to_string(),
        );
        let client = S3Client::new_with(
            rusoto_core::request::HttpClient::new().unwrap(),
            provider,
            storage.region,
        );
        bucket_list.push(Bucket {
            client,
            name: storage.bucket,
        });
    }
    let bucket_state = Arc::new(AppBucketList {
        buckets: bucket_list,
    });
    let http_server = HttpServer::new(move || {
        App::new()
            .data(bucket_state.clone())
            .service(ok)
            .default_service(web::get().to(object_req))
    })
    .bind(
        settings
            .get_str("host")
            .unwrap_or("127.0.0.1:8080".to_string()),
    )?
    .run();
    println!(
        "Start: {}",
        settings
            .get_str("host")
            .unwrap_or("127.0.0.1:8080".to_string())
    );
    http_server.await
}
