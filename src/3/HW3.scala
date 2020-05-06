import org.apache.spark.{SparkConf, SparkContext}

object HW3 {
  def main(args: Array[String]) {

    val conf = new SparkConf().setMaster("local").setAppName("t1")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

    val page_links = sc.parallelize(
      List(
        ('A', List('B', 'C')),
        ('B', List('A', 'C')),
        ('C', List('A','B', 'D')),
        ('D', List('C'))
      ))

    var rank1 = page_links.mapValues(_ => 1.0) //(A, 1)
    for (i <- 1 to 20) {
      val rank2 = page_links.join(rank1) //(A, (List, 1))
      val rank3 = rank2.values.flatMap {
        case (links, v) => links.map((_, v / links.size)) //(B, v / len)
      }
      rank1 = rank3.reduceByKey(_ + _).mapValues(v => 0.15 + 0.85 * v)
      rank1.sortByKey(ascending = true).collect().foreach(p => println(p._1 + " rank=" + p._2))
      println(i)
    }
  }
}
