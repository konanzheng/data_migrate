

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
    } else {
        println!("运行需要数据库连接字符串,例如: ./data_migrate.exe {}", url);
        return Ok(());
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
    // 循环查询pid ,每次查1000条数据,pid %100 后放到对应的集合中，然后循环集合更新数据，每个集合对应一个表，一个insert语句
    let m_row = sqlx::query("select FLOOR(min(pid)) as min_pid,FLOOR(max(pid)) as max_pid from s_sxjchistorymark").fetch_one(&pool).await?;
    let min_pid:u32 = m_row.get_unchecked("min_pid");
    let max_pid:u32 = m_row.get_unchecked("max_pid");
    let mut curr_pid:usize= min_pid as usize;
    while curr_pid< max_pid as usize {
        let sql = format!("select FLOOR(pid) as pid from s_sxjchistorymark where pid >= {}  group by pid order by pid limit {}",curr_pid,1000);
        let mut handles2 = Vec::with_capacity(100);
        let mut pids = Vec::with_capacity(100);
        for i in 0..100 {
            pids.push(Vec::<usize>::new());
        }
        let pid_rows = sqlx::query(&sql).fetch_all(&pool).await?;
        for row in  pid_rows {
            let pid:u32 = row.get_unchecked("pid");
            curr_pid = pid as usize;
            pids[curr_pid%100].push(curr_pid);
        }
        for i in 0..100{
            handles2.push(tokio::spawn(execute(pids.get(i).unwrap().to_vec(),i,pool.clone())));
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
async fn execute(pid: Vec::<usize>,mod_pid:usize,pool2:MySqlPool ){
    let mut pid_where = "-1".to_string();
    for p in pid {
        pid_where = format!("{},{}",pid_where,p);
    }
    let update_sql = format!("insert into s_sxjchistorymark{} select * from s_sxjchistorymark where pid in ({}) ",mod_pid,pid_where);
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
}