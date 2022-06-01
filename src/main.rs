

use sqlx::{Executor, MySqlPool, mysql,Row};
use sqlx::pool::PoolConnection;
use sqlx::types::*;
use sqlx::mysql::MySqlRow;
use std::time::Instant;
use std::time::Duration;
use std::env;

#[tokio::main]
async fn main() -> Result<(), sqlx::Error>{
    let args: Vec<String> = env::args().collect();
    let mut url = "mysql://hams:hams@localhost:3306/ylj";
    // let mut url="";
    if args.len() > 1 {
        url = &args[1];
    }
    let now = Instant::now();
    const SIZE:u32 = 100;
    let pool = mysql::MySqlPoolOptions::new().min_connections(SIZE).max_connections(SIZE).connect_timeout(Duration::from_secs(30)).idle_timeout(Duration::from_secs(10)).connect(url).await?;
    println!("db_pool is : {:?}", pool);
    // 删除 表中数据
    let mut handles = Vec::with_capacity(100);
    for i in 0..100 {
        let pool2 = pool.clone();
        handles.push(tokio::spawn(truncate(i,pool2)));  
    }
    for handle in handles {
        handle.await;
    }
    // 循环查询pid ,根据pid 更新数据,每次查二十条数据
    // let m_row = sqlx::query("select min(pid) as min_pid,max(pid) as max_pid from s_sxjchistorymark").fetch_one(&pool).await?;
    let min_pid = 6985;
    let max_pid = 28872;
    let mut curr_pid:u32= 6985;
    while curr_pid< max_pid {
        let sql = format!("select FLOOR(pid) as pid from s_sxjchistorymark where pid >= {}  group by pid order by pid limit {}",curr_pid,100);
        let mut handles2 = Vec::with_capacity(100);
        let pid_rows = sqlx::query(&sql).fetch_all(&pool).await?;
        for row in  pid_rows {
            curr_pid= row.get_unchecked("pid");
            handles2.push(tokio::spawn(execute(curr_pid,pool.clone())));
        }
        for handle in handles2 {
            handle.await;
        }
        println!("当前已处理pid: {}",curr_pid);
    }    
    pool.close().await;
    println!("程序结束 db_pool size: {},num_idle: {}, is_closed: {}", pool.size(), pool.num_idle(), pool.is_closed());
    println!("总耗时：{} ms", now.elapsed().as_millis());
    Ok(())
}
async fn execute(pid: u32,pool2:MySqlPool ){
    let mod_pid = pid%100;
    let update_sql = format!("insert into s_sxjchistorymark{} select * from s_sxjchistorymark where pid = {} ",mod_pid,pid);
    // sqlx::query(&update_sql).execute(&pool2).await;
    pool2.execute(sqlx::query(&update_sql)).await;
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