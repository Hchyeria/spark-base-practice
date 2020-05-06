import org.apache.spark.{SparkConf, SparkContext}

object Sogou {
  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local").setAppName("sogou")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

    val path = "./src/4/SogouQ.mini"
    val rawRdd = sc.textFile(path)

    println("count: " + rawRdd.count())

    val filterData = rawRdd.map(_.split("\\s+")).filter(_.length == 6)
    val rdd3 = filterData.filter(a => a(3).toInt == 1 && a(4).toInt == 1)
    val rdd4 = rdd3.map(x => (x(1), 1)).reduceByKey(_ + _).sortBy(-_._2)

    rdd4.foreach(println)
    rdd4.cache()
    rdd4.saveAsTextFile("./src/4/result")
  }
}
