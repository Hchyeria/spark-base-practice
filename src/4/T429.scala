import org.apache.spark.{SparkConf, SparkContext}

object T429 {
  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local").setAppName("test")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

    val a = sc.parallelize(List("A", "mk", "cd", "abc", "zp"))
    val b = a.keyBy(_.length)
    val c = b.combineByKey(
      v => List(v),
      (mv: List[String], v: String) => v +: mv,
      (mc1: List[String], mc2: List[String]) => mc1 ::: mc2
    )
    c.foreach(println)
  }
}
