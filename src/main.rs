use std::collections::HashMap;
use std::sync::Arc;

use actix_web::{get, web, App, HttpRequest, HttpResponse, HttpServer, Responder};

use opendal::Operator;
use config::Value;

struct Bucket {
    operator: Operator,
}

struct AppBucketList {
    buckets: Vec<Bucket>,
}

#[get("/")]
async fn ok() -> impl Responder {
    HttpResponse::Ok().body("OK")
}

async fn get_object(bucket: &Bucket, key: &str) -> Option<Vec<u8>> {
    let key = if key.starts_with('/') {
        &key[1..]
    } else {
        key
    };
    
    if let Ok(data) = bucket.operator.read(key).await {
        return Some(data.to_vec());
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

fn parse_bucket_config(cfg: HashMap<String, Value>) -> HashMap<String, String> {
    let mut config = HashMap::new();
    
    if let Some(access_key) = cfg.get("access-key") {
        config.insert("access_key_id".to_string(), access_key.to_string());
    }
    
    if let Some(secret_key) = cfg.get("secret-key") {
        config.insert("secret_access_key".to_string(), secret_key.to_string());
    }
    
    if let Some(endpoint) = cfg.get("endpoint") {
        let endpoint_str = endpoint.to_string();
        if !endpoint_str.is_empty() {
            config.insert("endpoint".to_string(), endpoint_str);
        }
    }
    
    if let Some(region) = cfg.get("region") {
        config.insert("region".to_string(), region.to_string());
    } else {
        config.insert("region".to_string(), "us-east-1".to_string());
    }
    
    config
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let settings = config::Config::builder()
        .add_source(config::File::with_name("config"))
        .build()
        .unwrap();
    let config_bucket_list = settings.get_array("bucket").unwrap_or(Vec::new());
    let mut bucket_list: Vec<Bucket> = Vec::with_capacity(config_bucket_list.capacity());
    
    for bucket in config_bucket_list {
        let t = bucket.into_table().unwrap();
        let bucket_name = t
            .get("bucket")
            .unwrap_or(&Value::from("bucket"))
            .to_string();
        let config = parse_bucket_config(t);
        
        let mut builder = opendal::services::S3::default();
        
        if let Some(access_key) = config.get("access_key_id") {
            builder = builder.access_key_id(access_key);
        }
        if let Some(secret_key) = config.get("secret_access_key") {
            builder = builder.secret_access_key(secret_key);
        }
        if let Some(region) = config.get("region") {
            builder = builder.region(region);
        }
        if let Some(endpoint) = config.get("endpoint") {
            builder = builder.endpoint(endpoint);
        }
        
        builder = builder.bucket(bucket_name.as_str());
        
        let operator = Operator::new(builder)
            .expect("Failed to create S3 operator")
            .finish();
        
        bucket_list.push(Bucket {
            operator,
        });
    }
    
    let bucket_state = Arc::new(AppBucketList {
        buckets: bucket_list,
    });
    let http_server = HttpServer::new(move || {
        App::new()
            .app_data(web::Data::new(bucket_state.clone()))
            .service(ok)
            .default_service(web::get().to(object_req))
    })
    .bind(
        settings
            .get_string("host")
            .unwrap_or("127.0.0.1:8080".to_string()),
    )?
    .run();
    println!(
        "Start: {}",
        settings
            .get_string("host")
            .unwrap_or("127.0.0.1:8080".to_string())
    );
    http_server.await
}
