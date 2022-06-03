use sqlx::{Executor, MySqlPool, mysql,Row,ConnectOptions};
use sqlx::mysql::MySqlConnectOptions;
use std::str::FromStr;
use std::time::Instant;
use std::time::Duration;
use std::env;
// use simple_log::log::{info, error};
use simple_log::LogConfig;
#[macro_use]
extern crate simple_log;
#[tokio::main]
async fn main() -> Result<(), sqlx::Error>{
    let config = r#"
    {
        "path":"./log/data_migrate.log",
        "level":"info",
        "size":10,
        "out_kind":["console","file"],
        "roll_count":10,
        "time_format":"%H:%M:%S.%f"
    }"#;
    let log_config: LogConfig = serde_json::from_str(config).unwrap();

    simple_log::new(log_config).unwrap();//init log
    let args: Vec<String> = env::args().collect();
    let mut url = String::new();
    if args.len() > 1 {
        url = String::from(args[1].as_str());
    } else {
        info!("开始执行前确认下列三件事,确认后请输入数据库连接字符串,格式: mysql://user:pass@host:port/dbname,例如: mysql://hams:hams@localhost:3306/ylj");
        info!("A. 四性检测mark分表已生成100张表结构:S_SXJCHISTORYMARK0到S_SXJCHISTORYMARK99");
        info!("B. 处理历史数据过程中不能操作四性检测相关任何功能，包括新旧接口。");
        info!("C. 数据库连接字符串是正式环境或者测试环境请确认清楚。");
        std::io::stdin().read_line(&mut url).unwrap();
    }
    let pool :MySqlPool ;
    loop {
        let mut opts = MySqlConnectOptions::from_str(url.as_str()).unwrap();
        opts.disable_statement_logging();
        let c =  mysql::MySqlPoolOptions::new().min_connections(SIZE).max_connections(SIZE).connect_timeout(Duration::from_secs(60)).idle_timeout(Duration::from_secs(10))
        .connect_with(opts).await;
        match c {
            Ok(r) => {
                pool = r;
                info!("数据库连接成功: {} 开始处理", url);
                break;
            },
            Err(e) => {
                info!("字符串:{} 连接失败,详细错误信息:{:?},\n请重新输入连接字符串",url,e);
                url.clear();
                std::io::stdin().read_line(&mut url).unwrap();
            }
        }
    }
    let now = Instant::now();
    let now2 = Instant::now();
    const SIZE:u32 = 100;
    info!("数据库连接池 is : {:?}", pool);
    // 删除 表中数据
    let mut handles = Vec::with_capacity(100);
    for i in 0..100 {
        let pool2 = pool.clone();
        handles.push(tokio::spawn(truncate(i,pool2)));  
    }
    for handle in handles {
        let _r = handle.await;
    }
    println!("\n清空数据耗时：{}s, 开始处理分表数据",now2.elapsed().as_secs());
    // 循环查询pid ,每次查1000条数据,pid %100 后放到对应的集合中，然后循环集合更新数据，每个集合对应一个表，一个insert语句
    let m_row = sqlx::query("select FLOOR(min(pid)) as min_pid,FLOOR(max(pid)) as max_pid from s_sxjchistorymark").fetch_one(&pool).await?;
    let min_pid:u32 = m_row.get_unchecked("min_pid");
    let max_pid:u32 = m_row.get_unchecked("max_pid");
    let mut curr_pid:usize= min_pid as usize;
    while curr_pid< max_pid as usize {
        let sql = format!("select FLOOR(pid) as pid from s_sxjchistorymark where pid >= {}  group by pid order by pid limit {}",curr_pid,1000);
        let mut handles2 = Vec::with_capacity(100);
        let mut pids = Vec::with_capacity(100);
        for _i in 0..100 {
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
            let _r = handle.await;
        }
        println!("数据处理进度(当前pid/最大pid): {}/{}",curr_pid,max_pid);
        curr_pid +=1;
    }    
    pool.close().await;
    info!("程序结束 总耗时：{} ms", now.elapsed().as_secs());
    Ok(())
}
async fn execute(pid: Vec::<usize>,mod_pid:usize,pool2:MySqlPool ){
    let mut pid_where = "-1".to_string();
    for p in pid {
        pid_where = format!("{},{}",pid_where,p);
    }
    let update_sql = format!("insert into s_sxjchistorymark{} select * from s_sxjchistorymark where pid in ({}) ",mod_pid,pid_where);
    let _r = pool2.execute(sqlx::query(&update_sql)).await;
    match _r {
        Ok(_) => {
            // println!("执行sql:{} 成功",update_sql);
        },
        Err(e) => {
            error!("执行sql:{} 失败,错误信息:{:?}",update_sql,e);
        }
    }
}
async fn truncate(i: u8,pool2:MySqlPool ){
    let now = Instant::now();
    let mut conn = pool2.acquire().await.unwrap();
    let del = format!("truncate S_SXJCHISTORYMARK{}",i);
    let q = conn.execute(sqlx::query(&del)).await;
    match q {
        Ok(_) => {
            println!("清空表{}成功,耗时:{} ms",i,now.elapsed().as_secs());
        },
        Err(e) => {
            error!("清空表{}失败,sql:{}详细错误信息:{:?}",i,del,e);
        }
    }
}