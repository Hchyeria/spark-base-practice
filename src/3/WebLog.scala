import org.apache.spark.{SparkConf, SparkContext}

object WebLog {
  def main(args: Array[String]) {
      val conf = new SparkConf().setMaster("local").setAppName("test")
      val sc = new SparkContext(conf)
      sc.setLogLevel("WARN")

      val path = "./src/3/weblog.txt"
      val raw_rdd = sc.textFile(path)

      val data_rdd = raw_rdd.zipWithIndex().filter(_._2 > 3).map(_._1)
      val data_rdd1 = data_rdd.map(_.split("\\s+"))
      println(data_rdd1.count())

      val data_rdd2 = data_rdd1.filter(_.length == 11)
      data_rdd2.map(x => (x(7), 1)).reduceByKey(_ + _).sortBy(-_._2).collect().take(10).foreach(println)

      val data_rdd3 = data_rdd2.map(x => x(7) + ":" + x(2)).distinct()
      val data_rdd4 = data_rdd3.map(s => (s.split(":")(0), s.split(":")(1))).groupByKey()
      val data_rdd5 = data_rdd3.map{s =>
          val arr = s.split(":")
          val c7 = arr(0)
          var ip = arr(1)
          (c7, 1)
      }.reduceByKey(_ + _)
      val data_rdd6 = data_rdd5.join(data_rdd4)
      data_rdd6.sortBy(-_._2._1).take(num = 10).foreach {
        case (p, (c, it)) => println("网页=" + p + "  count=" + c + "  ips=" + it.mkString("[", ";", "]"))
      }


  }
}
