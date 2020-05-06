import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir", "G:\\big-data\\hadoop-3.1.3")
    val inputFile =  "./src/WordCount/README.md"
    val conf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(conf)
    val textFile = sc.textFile(inputFile)
    textFile.flatMap(line => line.split("\\W+"))
      .map(word => (word, 1)).reduceByKey((a, b) => a + b)
        .sortBy(-_._2).collect().foreach(t => println(t._1 + " " + t._2))
  }



}