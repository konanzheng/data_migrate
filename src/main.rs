

use sqlx::{Executor, MySqlPool, mysql};
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
    let mut handles = Vec::with_capacity(100);
    let mut handles2 = Vec::with_capacity(100);
    let pool = mysql::MySqlPoolOptions::new().min_connections(50).max_connections(50).connect_timeout(Duration::from_secs(420)).connect(url).await?;
    println!("db_pool is : {:?}", pool);
    for i in 0..1 {
        let pool2 = pool.clone();
        handles.push(tokio::spawn(truncate(i,pool2)));  
        let pool3 = pool.clone();
        handles2.push(tokio::spawn(insert(i,pool3)));  
    }
    for handle in handles {
        handle.await;
    }
    for handle in handles2 {
        handle.await;
    }
    pool.close().await;
    println!("总耗时：{} ms", now.elapsed().as_millis());
    Ok(())
}
async fn truncate(i: u8,pool2:MySqlPool ){
    let now = Instant::now();
    pool2.acquire().await;
    let del = format!("truncate S_SXJCHISTORYMARK{}",i);
    let mut q = pool2.execute(sqlx::query(&del)).await;
    match q {
        Ok(r) => {
            println!("清空数据成功{},{:?},耗时:{} ms",i,r,now.elapsed().as_millis());
        },
        Err(e) => {
            println!("清空数据失败{},详细错误信息:{:?}",i,e);
        }
    }
}
async fn insert(i: u8,pool2:MySqlPool ){
    let now = Instant::now();
    // pool2.acquire().await;
    let did_sql = format!("select @mark_did_{}:=0",i);
    let mut q=pool2.execute(sqlx::query(&did_sql)).await; 
    let sql = format!("insert into s_sxjchistorymark{} select @mark_did_{}:=@mark_did_{}+1 as DID ,PID,JCCODE,JCNAME,FIELDNAME,CHNAME,AW,JCLB,JCDX,RESULT,BZ,UPDATEREASON,UPDATOR,UPDATETIME,UPDATEMEMO,ISMANUAL  from s_sxjchistorymark where pid %100 = {}",i,i,i,i);
    q=pool2.execute(sqlx::query(&sql)).await;
    match q {
        Ok(r) => {
            println!("插入数据成功{},{:?},耗时:{} ms",i,r,now.elapsed().as_millis());
        },
        Err(e) => {
            println!("插入数据失败{},详细错误信息:{:?}",i,e);
            println!("建议手动在mysql中执行sql:{}\n{}",did_sql,sql);
        }
    }

}
