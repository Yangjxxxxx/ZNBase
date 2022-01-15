![ZNBaseDB](docs/media/d_db.png?raw=true "ZNBaseDB logo")




[![TeamCity CI](https://teamcity.znbasedb.com/guestAuth/app/rest/builds/buildType:(id:ZNBASE_UnitTests)/statusIcon.svg)](https://teamcity.znbasedb.com/viewLog.html?buildTypeId=ZNBASE_UnitTests&buildId=lastFinished&guest=1)
[![GoDoc](https://godoc.org/github.com/znbasedb/znbase?status.svg)](https://godoc.org/github.com/znbasedb/znbase)

- [What is ZNBaseDB?](#what-is-znbasedb)
- [Docs](#docs)
- [Quickstart](#quickstart)
- [Client Drivers](#client-drivers)
- [Deployment](#deployment)
- [Need Help?](#need-help)
- [Contributing](#contributing)
- [Design](#design)
- [Comparison with Other Databases](#comparison-with-other-databases)
- [See Also](#see-also)

# 浪潮云海分布式数据库是浪潮研发的新一代的云原生数据库，具有云原生、强一致、可扩展等先进特性


- [什么是云海分布式数据库?](#什么是云海分布式数据库)
- [状态](#状态)
- [任务管理](#任务管理)
- [项目文档](#项目文档)
- [客户端接口](#客户端接口)
- [分支管理?](#分支管理)
- [本地编译](#本地编译)
- [Devops](#Devops)


For more details, see our [FAQ](https://znbaselabs.com/docs/stable/frequently-asked-questions.html) or [architecture document](
https://www.znbaselabs.com/docs/stable/architecture/overview.html).
## 什么是云海分布式数据库?

云海分布式数据库是支持事务和强一致的，有多种存储引擎的分布式数据库产品。
支持水平扩展，能够在磁盘、主机、机柜甚至数据中心故障的情况下不丢失任何数据。
支持强一致的同时提供标准SQL接口。


## 状态

云海分布式数据库即将在2020年3月发布第一个正式版本

ZNBaseDB is production-ready. See our
[Roadmap](https://github.com/znbasedb/znbase/wiki/Roadmap) for a list of features planned or in development.

## 任务管理

本项目采用Jira进行任务管理，请登陆并查看 [newsql jira](http://10.10.7.2:8080/secure/RapidBoard.jspa?rapidView=135&projectKey=NEWSQL&view=planning.nodetail)

For guidance on installation, development, deployment, and administration, see our [User Documentation](https://znbaselabs.com/docs/stable/).

## 项目文档

本项目的文档记录在 [Confluence](http://10.10.7.5:8090/pages/viewpage.action?pageId=17540033)

1. [Install ZNBaseDB](https://www.znbaselabs.com/docs/stable/install-znbasedb.html).

1. [Start a local cluster](https://www.znbaselabs.com/docs/stable/start-a-local-cluster.html)
   and talk to it via the [built-in SQL client](https://www.znbaselabs.com/docs/stable/use-the-built-in-sql-client.html).

1. [Learn more about ZNBaseDB SQL](https://www.znbaselabs.com/docs/stable/learn-znbasedb-sql.html).

1. Use a PostgreSQL-compatible driver or ORM to
   [build an app with ZNBaseDB](https://www.znbaselabs.com/docs/stable/build-an-app-with-znbasedb.html).

1. [Explore core features](https://www.znbaselabs.com/docs/stable/demo-data-replication.html),
   such as data replication, automatic rebalancing, and fault tolerance and recovery.

## 客户端接口

浪潮云海数据库兼容PostgreSQL协议,因此您可以使用任意的您熟悉的语言中的PostgreSQL驱动来访问该数据库

## 分支管理


## 研发环境构建

## Devops

ZNBaseDB supports the PostgreSQL wire protocol, so you can use any available PostgreSQL client drivers to connect from various languages.

- For recommended drivers that we've tested, see [Install Client Drivers](https://www.znbaselabs.com/docs/stable/install-client-drivers.html).

- For tutorials using these drivers, as well as supported ORMs, see [Build an App with ZNBaseDB](https://www.znbaselabs.com/docs/stable/build-an-app-with-znbasedb.html).

## Deployment

- [Test Deployment](https://www.znbaselabs.com/docs/stable/deploy-a-test-cluster.html) - Easiest way to test an insecure, multi-node ZNBaseDB cluster.
- Production Deployments
    - [Manual](https://www.znbaselabs.com/docs/stable/manual-deployment.html) - Steps to deploy a ZNBaseDB cluster manually on multiple machines.
    - [Cloud](https://www.znbaselabs.com/docs/stable/cloud-deployment.html) - Guides for deploying ZNBaseDB on various cloud platforms.
    - [Orchestration](https://www.znbaselabs.com/docs/stable/orchestration.html) - Guides for running ZNBaseDB with popular open-source orchestration systems.

## Need Help?

- [Troubleshooting documentation](https://www.znbaselabs.com/docs/stable/troubleshooting-overview.html) -
  Learn how to troubleshoot common errors, cluster and node setup, and SQL query behavior,
  and how to use debug and error logs.

- [ZNBaseDB Forum](https://forum.znbaselabs.com/) and
  [Stack Overflow](https://stackoverflow.com/questions/tagged/znbasedb) - Ask questions,
  find answers, and help other users.

- For filing bugs, suggesting improvements, or requesting new features, help us out by
  [opening an issue](https://github.com/znbasedb/znbase/issues/new).

## Contributing

We're an open source project and welcome contributions. See our [Contributing Guide](https://www.znbaselabs.com/docs/stable/contribute-to-znbasedb.html) for more details.

Engineering discussion takes place on our public mailing list,
[znbase-db@googlegroups.com](https://groups.google.com/forum/#!forum/znbase-db).


## Design

For an in-depth discussion of the ZNBaseDB architecture, see our [Architecture Guide](https://www.znbaselabs.com/docs/stable/architecture/overview.html).

For the original design motivation, see our [design doc](https://github.com/znbasedb/znbase/blob/master/docs/design.md).


## Comparison with Other Databases

To see how key features of ZNBaseDB stack up against other databases,
visit the [ZNBaseDB in Comparison](https://www.znbaselabs.com/docs/stable/znbasedb-in-comparison.html) page on our website.

## See Also

- [Tech Talks](https://www.znbaselabs.com/community/tech-talks/) by ZNBaseDB founders and engineers
- [The ZNBaseDB User documentation](https://znbaselabs.com/docs/stable/)
- [The ZNBaseDB Blog](https://www.znbaselabs.com/blog/)
- Key Design documents:
  - [Serializable, Lockless, Distributed: Isolation in ZNBaseDB](https://www.znbaselabs.com/blog/serializable-lockless-distributed-isolation-znbasedb/)
  - [Consensus, Made Thrive](https://www.znbaselabs.com/blog/consensus-made-thrive/)
  - [Trust, But Verify: How ZNBaseDB Checks Replication](https://www.znbaselabs.com/blog/trust-but-verify-znbasedb-checks-replication/)
  - [Living Without Atomic Clocks](https://www.znbaselabs.com/blog/living-without-atomic-clocks/)
  - [The ZNBaseDB Architecture Document](https://github.com/znbasedb/znbase/blob/master/docs/design.md)
  
