## 如何实现分布式关系型数据库
似乎从2015以后，随着几篇论文的发布，分布式数据存储技术得到了突破，国内外NewSQL开源数据库如下饺子般一个接着一个的出现。本人对分布式关系型数据库技术的关注源自于曾经研制sharding中间件所遇到的一些痛点，毕竟sharding中间件能实现的功能是有限的，突破不了。NewSQL自然是解决这些问题最合理的方案。然而分布式NewSQL数据库如何优雅的解决的数据存储、一致性、负载均衡、动态增减结点、分布式事务问题是有挑战的，项目的目的为了探索这些功能的实现。
#### 从分布式存储开始
###### 相关理论paper
* 数据一致性算法[raft](https://raft.github.io/),[paxos](https://github.com/Tencent/phxpaxos)
* [Google spanner](http://dblab.xmu.edu.cn/wp-content/uploads/2012/09/20120920_163800_492.pdf)
* Google F1
* [Apache Kudu](http://kudu.apache.org/kudu.pdf)
###### 分式存储引擎开源项目
* [tikv](https://pingcap.com/blog-cn/building-distributed-db-with-raft/)弹性伸缩的存储系统
* [elasticell](https://github.com/deepfabric/elasticell) 参考tikv go语言的实现
* [Apache Kudu](https://github.com/apache/kudu) c++实现
