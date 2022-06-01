

use sqlx::{Executor, MySqlPool, mysql};
use sqlx::pool::PoolConnection;
use std::time::Instant;
use std::time::Duration;
use std::env;

#[tokio::main]
async fn main() -> Result<(), sqlx::Error>{
    let args: Vec<String> = env::args().collect();
    // let mut url = "mysql://hams:hams@localhost:3306/ylj";
    let mut url="";
    if args.len() > 1 {
        url = &args[1];
    }
    let now = Instant::now();
    const SIZE:u32 = 10;
    let pool = mysql::MySqlPoolOptions::new().min_connections(SIZE).max_connections(SIZE).connect_timeout(Duration::from_secs(30)).idle_timeout(Duration::from_secs(10)).connect(url).await?;
    println!("db_pool is : {:?}", pool);
    if args.len()>2 {
        let mut handles = Vec::with_capacity(100);
        for i in 0..100 {
            let pool2 = pool.clone();
            handles.push(tokio::spawn(truncate(i,pool2)));  
        }
        for handle in handles {
            handle.await;
        }
    } else {
        let batch = 100/SIZE;
        for s in 0..batch {
            let mut handles2 = Vec::with_capacity(SIZE.try_into().unwrap());
            for i in 0..SIZE {
                let pool3 = pool.clone();
                handles2.push(tokio::spawn(insert((i+s*SIZE).try_into().unwrap(),pool3)));  
            }
            for handle in handles2 {
                handle.await;
            }
        }
    }
    
    pool.close().await;
    println!("程序结束 db_pool size: {},num_idle: {}, is_closed: {}", pool.size(), pool.num_idle(), pool.is_closed());
    println!("总耗时：{} ms", now.elapsed().as_millis());
    Ok(())
}
async fn truncate(i: u8,pool2:MySqlPool ){
    let now = Instant::now();
    let mut conn = pool2.acquire().await.unwrap();
    let del = format!("truncate S_SXJCHISTORYMARK{}",i);
    let q = conn.execute(sqlx::query(&del)).await;
    match q {
        Ok(r) => {
            println!("清空表{}成功,{:?},耗时:{} ms",i,r,now.elapsed().as_millis());
        },
        Err(e) => {
            println!("清空表{}失败,详细错误信息:{:?}",i,e);
        }
    }
    // conn.detach();
    println!("db_pool size: {},num_idle: {}", pool2.size(), pool2.num_idle());
}
async fn insert(i: u8,pool2:MySqlPool ){
    let now = Instant::now();
    let mut conn = pool2.acquire().await.unwrap();
    conn.execute(sqlx::query("set session transaction isolation level read committed;")).await;
    // let did_sql = format!("select @mark_did:=0");
    // let mut q=pool2.execute(sqlx::query(&did_sql)).await; 
    // let sql = format!("insert into s_sxjchistorymark{} select @mark_did:=@mark_did+1 as DID ,PID,JCCODE,JCNAME,FIELDNAME,CHNAME,AW,JCLB,JCDX,RESULT,BZ,UPDATEREASON,UPDATOR,UPDATETIME,UPDATEMEMO,ISMANUAL  from s_sxjchistorymark where pid %100 = {}",i,i);
    let sql = format!("insert into s_sxjchistorymark{} select * from s_sxjchistorymark where pid %100 = {}",i,i);
    let q = conn.execute(sqlx::query(&sql)).await;
    match q {
        Ok(r) => {
            println!("插入表{}成功,{:?},耗时:{} ms",i,r,now.elapsed().as_millis());
        },
        Err(e) => {
            println!("插入表{}据失败,详细错误信息:{:?}\n建议手动在mysql中执行sql:\n{};",i,e,sql);
        }
    }
    // conn.detach();
    println!("db_pool size: {},num_idle: {}", pool2.size(), pool2.num_idle());
}
