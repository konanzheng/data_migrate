## 数据迁移

单表数据量太大，需要迁移到分表。写脚本迁移到分表执行太慢不能并行，写了个小程序并行处理。学习一下sqlx的使用 和rust中tokio并行任务的使用。

业务背景：

表结构： did,pid, 其他业务字段 

分表规则: 源表名+(pid %100 )



#### 使用slqx连接mysql中进行数据处理

1. 查出要处理的数据的最小pid,和最大pid
2. 从最小pid开始每次最多查询1000条数据pid数据,limit 1000 
3. 根据分表规则把 pid 放到 100个分表 对应的 pid字符串,并记录当前pid
4. 循环100个分表组装insert sql, insert into 分表 select * from 原表 where pid in pid字符串
5. 并行执行100个insert sql 
6. 循环以上过程直到当前pid 超过最大pid 结束

#### 使用action 自动构建linux版本程序并发布到release

因为本地是windows环境 服务器是linux环境，本地搭建交叉编译太麻烦。所以想到github支持action就顺便学习一下通过action构建linux版本程序并发布到release。之前尝试了很多action 发现都是能上传到release但是不能下载后来发现因为项目是私有的所以无法下载解决办法就是改成public就行了。

```
name: Release

on:
  push:
    tags:
      - v[0-9]+.*

jobs:
  create-release:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - uses: taiki-e/create-gh-release-action@v1
        with:
          # (optional) Path to changelog.
          changelog: CHANGELOG.md
        env:
          # (required) GitHub token for creating GitHub Releases.
          GITHUB_TOKEN: ${{ secrets.GITHUB_TOKEN }}

  upload-assets:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - uses: taiki-e/upload-rust-binary-action@v1
        with:
          # (required) Binary name (non-extension portion of filename) to build and upload.
          bin: data_migrate
        env:
          # (required) GitHub token for uploading assets to GitHub Releases.
          GITHUB_TOKEN: ${{ secrets.GITHUB_TOKEN }}
```