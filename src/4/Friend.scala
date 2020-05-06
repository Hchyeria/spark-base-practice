import org.apache.spark.{SparkConf, SparkContext}

object Friend {
  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local").setAppName("friend")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

    val path = "./src/4/fs.txt"
    val rawRdd = sc.textFile(path)
    val friendList = rawRdd.flatMap(line => {
      val list = line.split(",")
      val user = list(0)
      val friends = list(1).split(" ")

      val res = for {
        i <- friends.indices
        value = friends(i)
      } yield {
        val k = if (user(user.length - 1) < value(value.length - 1)) (user, value) else (value, user)
        (k, friends)
      }
      res
    })

    friendList.foreach(println)


    val rdd1 = friendList.groupByKey().mapValues(value => {
      val f = for {
        item <- value if value.nonEmpty;
        ele <- item
      } yield (ele, 1)

      f.groupBy(_._1).mapValues(_.unzip._2.sum)
        .filter(_._2 > 1).keys

    })


    rdd1.foreach(t => println(t._1 + " " + t._2.mkString(",")))
  }
}
