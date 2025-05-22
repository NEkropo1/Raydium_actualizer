// src/main.rs
use std::{fs::File, pin::Pin, sync::Arc};
use std::collections::HashMap;
use anyhow::Result;
use futures::{Stream, StreamExt};
use reqwest::Client;
use serde::Deserialize;

use arrow2::{
    array::{Float64Array, Utf8Array},
    chunk::Chunk,
    datatypes::{DataType, Field, Schema},
    io::parquet::write::{
        FileWriter, WriteOptions, CompressionOptions, RowGroupIterator, Encoding, Version,
    },
};

/// The bare‐bones of the v3 page response
#[derive(Deserialize)]
struct ApiPage<T> {
    success: bool,
    data: PageData<T>,
}
#[derive(Deserialize)]
struct PageData<T> {
    data: Vec<T>,
    hasNextPage: bool,
}

#[derive(Deserialize, Clone)]
struct MintInfo {
    #[serde(rename = "chainId")]   chain_id: u64,
    address: String,
    #[serde(rename = "programId")] program_id: String,
    #[serde(rename = "logoURI")]   logo_uri: String,
    symbol: String,
    name: String,
    decimals: u8,
    tags: Vec<String>,
    extensions: serde_json::Value,
}

/// Only the fields we actually care about
#[derive(Deserialize, Clone)]
struct PoolInfoV3 {
    id: String,
    price: f64,
    tvl: f64,
    #[serde(rename = "programId")]
    program_id: String,
    #[serde(rename = "mintA")]
    mint_a: MintInfo,
    #[serde(rename = "mintB")]
    mint_b: MintInfo,
}

/// Returns a Stream yielding each page's Vec<PoolInfoV3>, stopping when hasNextPage=false.
fn all_pools(
    base_url: impl Into<String>,
) -> Pin<Box<dyn Stream<Item = Result<Vec<PoolInfoV3>>> + Send>> {
    let base_url = base_url.into();
    let client = Client::new();

    Box::pin(async_stream::try_stream! {
        let mut page = 1;
        loop {
            let resp = client
                .get(&base_url)
                .query(&[
                ("poolType", "all".to_string()),
                ("poolSortField", "default".to_string()),
                ("sortType", "desc".to_string()),
                ("pageSize", 1000.to_string()),
                ("page", page.to_string())
            ])
                .send().await?
                .error_for_status()?
                .json::<ApiPage<PoolInfoV3>>().await?;

            if !resp.success {
                Err::<String, anyhow::Error>(anyhow::anyhow!("raydium returned success=false"))?;
            }

            let batch = resp.data.data;
            if batch.is_empty() {
                break;
            }
            yield batch.clone();

            if !resp.data.hasNextPage {
                break;
            }
            page += 1;
        }
    })
}

/// Write one big Parquet file with a single row‐group containing all of `all_pools`.
fn write_pools_parquet(all: &[PoolInfoV3], token_map: &HashMap<String, String>, path: &str) -> Result<()> {
    let ids:        Vec<&str> = all.iter().map(|p| p.id.as_str()).collect();
    let progs:      Vec<&str> = all.iter().map(|p| p.program_id.as_str()).collect();
    let prices:     Vec<f64>   = all.iter().map(|p| p.price).collect();
    let tvls:       Vec<f64>   = all.iter().map(|p| p.tvl).collect();
    let coin_mints: Vec<&str> = all.iter().map(|p| p.mint_a.address.as_str()).collect();
    let pc_mints:   Vec<&str> = all.iter().map(|p| p.mint_b.address.as_str()).collect();
    let symbols_a: Vec<&str> = all.iter()
        .map(|p| token_map
            .get(&p.mint_a.address)        // Option<&String>
            .map(|s| s.as_str())           // Option<&str>
            .unwrap_or("UNKNOWN"))         // &str
        .collect();

    let symbols_b: Vec<&str> = all.iter()
        .map(|p| token_map
            .get(&p.mint_b.address)
            .map(|s| s.as_str())
            .unwrap_or("UNKNOWN"))
        .collect();

    let id_arr    = Utf8Array::<i32>::from_slice(&ids);
    let prog_arr  = Utf8Array::<i32>::from_slice(&progs);
    let price_arr = Float64Array::from_slice(&prices);
    let tvl_arr   = Float64Array::from_slice(&tvls);
    let coin_mint_arr = Utf8Array::<i32>::from_slice(&coin_mints);
    let pc_mint_arr   = Utf8Array::<i32>::from_slice(&pc_mints);
    let symbols_a_arr = Utf8Array::<i32>::from_slice(&symbols_a);
    let symbols_b_arr = Utf8Array::<i32>::from_slice(&symbols_b);

    let schema = Schema::from(vec![
        Field::new("id",         DataType::Utf8,   false),
        Field::new("program_id", DataType::Utf8,   false),
        Field::new("price",      DataType::Float64, false),
        Field::new("tvl",        DataType::Float64, false),
        Field::new("coin_mint",         DataType::Utf8,   false),
        Field::new("pc_mint", DataType::Utf8,   false),
        Field::new("symbol_a",        DataType::Utf8,   false),
        Field::new("symbol_b",         DataType::Utf8,   false),
    ]);

    let chunk: Chunk<Arc<dyn arrow2::array::Array>> = Chunk::new(vec![
        Arc::new(id_arr)    as _,
        Arc::new(prog_arr)  as _,
        Arc::new(price_arr) as _,
        Arc::new(tvl_arr)   as _,
        Arc::new(coin_mint_arr)   as _,
        Arc::new(pc_mint_arr)   as _,
        Arc::new(symbols_a_arr)   as _,
        Arc::new(symbols_b_arr)   as _,
    ]);

    let mut file = File::create(path)?;
    let options = WriteOptions {
        write_statistics:    true,
        compression:         CompressionOptions::Snappy,
        version:             Version::V2,
        data_pagesize_limit: Some(1024*1024),
    };
    let mut writer = FileWriter::try_new(&mut file, schema.clone(), options)?;
    let num_cols = schema.fields.len();
    let encodings: Vec<Vec<Encoding>> = (0..num_cols)
        .map(|_| vec![ Encoding::Plain ])
        .collect();

    let row_groups = RowGroupIterator::try_new(
        std::iter::once(Ok(chunk)),
        &schema,
        options,
        encodings,
    )?;

    for group in row_groups {
        writer.write(group?)?;
    }
    writer.end(None)?;  // writes the footer/metadata

    Ok(())
}

async fn load_token_map(client: &Client) -> Result<HashMap<String, String>> {
    let url = "https://raw.githubusercontent.com/solana-labs/token-list/main/src/tokens/solana.tokenlist.json";
    let resp = client
        .get(url)
        .send().await?
        .error_for_status()?
        .json::<serde_json::Value>().await?;
    let mut map = HashMap::new();
    for tok in resp["tokens"].as_array().unwrap() {
        let address = tok["address"].as_str().unwrap().to_string();
        let symbol  = tok["symbol"].as_str().unwrap().to_string();
        map.insert(address, symbol);
    }
    Ok(map)
}

#[tokio::main]
async fn main() -> Result<()> {
    let mut pages = all_pools("https://api-v3.raydium.io/pools/info/list");
    let client    = Client::new();
    let token_map = load_token_map(&client).await?;
    let mut all    = Vec::new();
    while let Some(page) = pages.next().await {
        let page = page?;
        println!("fetched {} pools", page.len());
        all.extend(page);
    }
    println!("total pools = {}", all.len());

    write_pools_parquet(&all, &token_map, "pools.parquet")?;
    println!("✅ Wrote pools.parquet");

    Ok(())
}
