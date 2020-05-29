个人博客原文：[Spark的分区机制的应用及PageRank算法的实现](http://blog.taohuawu.club/article/spark-partition-pagerank)
> **佩奇排名**（PageRank），又称**网页排名**、**谷歌左侧排名**，是一种由[搜索引擎](https://zh.wikipedia.org/wiki/%E6%90%9C%E7%B4%A2%E5%BC%95%E6%93%8E)根据[网页](https://zh.wikipedia.org/wiki/%E7%BD%91%E9%A1%B5)之间相互的[超链接](https://zh.wikipedia.org/wiki/%E8%B6%85%E9%93%BE%E6%8E%A5)计算的技术，而作为网页排名的要素之一，以[Google公司](https://zh.wikipedia.org/wiki/Google%E5%85%AC%E5%8F%B8)创办人[拉里·佩奇](https://zh.wikipedia.org/wiki/%E6%8B%89%E9%87%8C%C2%B7%E4%BD%A9%E5%A5%87)（Larry Page）之姓来命名。[Google](https://zh.wikipedia.org/wiki/Google)用它来体现网页的相关性和重要性，在[搜索引擎优化](https://zh.wikipedia.org/wiki/%E6%90%9C%E7%B4%A2%E5%BC%95%E6%93%8E%E4%BC%98%E5%8C%96)操作中是经常被用来评估网页优化的成效因素之一。

# 概念

Spark中有一个很重要的特性是对数据集在节点间的分区进行控制，因为在分布式系统中，通信的代价是很大的，因此控制数据分布以获得最少的网络传输可以极大地提升整体性能，Spark程序可以通过控制RDD分区方式来减少通信开销。分区适用于那种基于类似join操作基于键的操作，并且一方的RDD数据是比较少变动且需要多次扫描的情况，这个时候可以对这个RDD做一个分区，最常用的是用Hash来进行分区，比如可以对RDD分100个区，此时spark会用每个键的hash值对100取模，然后把相同结果的放到同一个节点上。

## Spark分区的讲解

现在用一个例子（来自《Learning Spark: Lightning-Fast Big Data Analysis》一书）来说明一下：

```scala
// Initialization code; we load the user info from a Hadoop SequenceFile on HDFS.
// This distributes elements of userData by the HDFS block where they are found,
// and doesn't provide Spark with any way of knowing in which partition a
// particular UserID is located.
val sc = new SparkContext(...)
val userData = sc.sequenceFile[UserID, UserInfo]("hdfs://...").persist()

// Function called periodically to process a logfile of events in the past 5 minutes;
// we assume that this is a SequenceFile containing (UserID, LinkInfo) pairs.
def processNewLogs(logFileName: String) {
  val events = sc.sequenceFile[UserID, LinkInfo](logFileName)
  val joined = userData.join(events)// RDD of (UserID, (UserInfo, LinkInfo)) pairs
  val offTopicVisits = joined.filter {
    case (userId, (userInfo, linkInfo)) => // Expand the tuple into its components
      !userInfo.topics.contains(linkInfo.topic)
  }.count()
  println("Number of visits to non-subscribed topics: " + offTopicVisits)
}
```

上面的例子中，有两个RDD，userData的键值对是(UserID, UserInfo)，UserInfo包含了一个该用户订阅的主题的列表，该程序会周期性地将这张表与一个小文件进行组合，这个小文件中存着过去五分钟内某个网站各用户的访问情况，由(UserID, LinkInfo)。现在，我们需要对用户访问其未订阅主题的页面进行统计。可以通过Spark的join()操作来完成这个功能，其中需要把UserInfo和LinkInfo的有序对根据UserID进行分组，如上代码。

可以看出，因为每次调用processNewLogs()时都需要执行一次join()操作，但是数据具体的shuffle对我们来说却是不可控的，也就是我们不知道spark是如何进行分区的。spark默认在执行join()的时候会将两个RDD的键的hash值都算出来，然后将该hash值通过网络传输到同一个节点上进行相同键值的记录的连接操作，如下图所示：

![](https://www.safaribooksonline.com/library/view/learning-spark/9781449359034/assets/lnsp_0404.png)

因为userData这个RDD里面的数据是几乎不会变动的，或者说是极少会变动的，且它的内容也比events大很多，所以每次都要对它进行shuffle的话，是没有必要且浪费时间的，实际上只需要进行一次shuffle就可以了。

所以，可以通过预先分区来解决这个问题：在进行join()之前，对userData使用partitionBy()转化操作，把它变成一个哈希分区的RDD：

```scala
val sc = new SparkContext(...)
val userData = sc.sequenceFile[UserID, UserInfo]("hdfs://...")
                 .partitionBy(new HashPartitioner(100))   // Create 100 partitions
                 .persist()
```

调用partitionBy()之后，spark就可以预先知道这个RDD是已经进行过哈希分区的了，等到执行join()之时，它就会利用这一点：只对events进行shuffle，将events中特定UserID的记录发送到userData对应分区的机器节点上去。这样的话，就减少了大量的重复的网络通信，程序性能也会大大提高。改进后的程序的执行过程如下：

![](https://www.safaribooksonline.com/library/view/learning-spark/9781449359034/assets/lnsp_0405.png)

还有一点，你们可能注意到了新代码里最后还调用了一个persist()方法，这是另一个小优化点：对于那些数据不常变动且数据量较大的RDD，在进行诸如join()这种连接操作的时候尽量用persist()来做缓存，以提高性能。另外，分区数目的设置也有讲究，分区数目决定了这个任务在执行连接操作时的并行度，所以一般来说这个数目应该和集群中的总核心数保持一致。

最后，可能有人会问，能不能对events也进行分区进一步提高程序性能？这是没有必要的，因为events RDD是本地变量，每次执行都会更新，所以对它进行分区没有意义，即便对这种一次性变量进行分区，spark依然需要进行一次shuffle，所以，这是没有必要的。

## 使用分区来加快PageRank算法

PageRank算法是一种从RDD分区获益的更复杂的算法，下面我们用它为例来进一步讲解Spark分区的使用。

如果有不清楚的PageRank算法的具体实现的可以参考我以前的一篇文章：[hadoop下基于mapreduce实现pagerank算法](http://blog.taohuawu.club/article/9)

PageRank是一个迭代算法，因此它是一个能从RDD分区中获得性能加速的很好的例子，先上代码：

```scala
// Assume that our neighbor list was saved as a Spark objectFile
val links = sc.objectFile[(String, Seq[String])]("links")
              .partitionBy(new HashPartitioner(100))
              .persist()

// Initialize each page's rank to 1.0; since we use mapValues, the resulting RDD
// will have the same partitioner as links
var ranks = links.mapValues(v => 1.0)

// Run 10 iterations of PageRank
for (i <- 0 until 10) {
  val contributions = links.join(ranks).flatMap {
    case (pageId, (links, rank)) =>
      links.map(dest => (dest, rank / links.size))
  }
  ranks = contributions.reduceByKey((x, y) => x + y).mapValues(v => 0.15 + 0.85*v)
}

// Write out the final ranks
ranks.saveAsTextFile("ranks")
```

这个算法维护两个RDD，一个的键值对是(pageID, linkList)，包含了每个页面的出链指向的相邻页面列表（由pageID组成）；另一个的键值对是(pageID, rank)，包含了每个页面的当前权重值。算法流程如下：

1. 将每个页面的权重值初始化为1.0；

2. 在每次迭代中，对页面p，向其每个出链指向的页面加上一个rank(p)/neighborsSize(p)的贡献值contributionReceived；

3. 将每个页面的权重值设置为：0.15 + 0.85 * contributionReceived。

不断迭代步骤2和3，过程中算法会逐渐收敛于每个页面的实际PageRank值，实际运行之时大概迭代10+次以上即可。

算法将ranksRDD的每个元素的值设置为1.0，然后在每次迭代中不断更新ranks变量：首先对ranksRDD和静态的linksRDD进行一次join()操作，来获取每个页面ID对应的相邻页面列表和当前的权重值，然后使用flatMap创建出『contributions』来记录每个页面对各相邻页面的贡献值。然后再把这些贡献值按照pageID分别累加起来，把该页面的权重值设为0.15 + 0.85 * contributionsReceived。

接下来分析下上述代码做的的一些优化点：

1. linksRDD在每次迭代中都会和ranks发生连接操作，由于links是一个静态RDD（数据几乎不会变动），所以在一开始可以对它进行分区以减少网络shuffle，降低网络通信的开销。而且，linksRDD的字节数一般来说也会比ranks大很多，因为这个RDD包含了每个页面的出链指向的页面列表，类似于一个笛卡尔积的数量级。所以通过预先分区可以获得比原算法的普通MapReduce实现更好的性能；
2. 用persist()方法缓存RDD，使得在每次迭代里都可以复用，进一步提高性能；
3. 第一次创建ranks时，使用mapValues()而不是map()，保留了父RDD(links)的分区方式（因为map操作理论上可能会修改键值导致父RDD的分区不可用，所以map操作不保留父RDD的分区），这样第一次的join()操作的开销也会更小；
4. 在循环体中，调用reduceByKey()后使用mapValues()；因为reduceByKey()的结果已经是哈希分区的了，这样一来，下一次循环中将映射操作的结果再次与links进行连接时就会更加高效。

# 参考

[https://www.safaribooksonline.com/library/view/learning-spark/9781449359034/ch04.html](https://www.safaribooksonline.com/library/view/learning-spark/9781449359034/ch04.html)