import org.apache.spark.{SparkConf, SparkContext}

object Weather {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("weather")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

    val path = "./src/5/weather.txt"
    val rawRdd = sc.textFile(path)

    rawRdd.map(line => {
      val year = line.substring(15, 19).toInt
      val weather = line.substring(45, 50).toInt
      val q = line.substring(50, 51)
      (year, weather, q)
    }).filter(a => a._2 != 9999 && a._3.matches("[01459]"))
      .map(a => (a._1, a._2))
      .reduceByKey(math.max)
      .foreach(x => {
        println("year: " + x._1 + " max: " + x._2)
      })
  }
}
