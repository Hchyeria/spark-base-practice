import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.Map

object Movie {
  def main(args: Array[String]) {
    val userDataPath = "./src/6/u.data"
    val itemDataPath = "./src/6/u.item"

    val (ratingRDD, movie) = prepareData(userDataPath, itemDataPath)
    val (trainData, validationData, testData) = prepareTrainData(ratingRDD)

    trainData.cache()
    validationData.cache()
    testData.cache()

    val (bestModel, rmse, pData) = trainValidation(trainData, validationData)
    val testRmse=  computeRmse(bestModel, testData)
    println(pData)
    println("tranRmse: " + rmse + "testRmse: " + testRmse)


    // train model
    recommendMovies(bestModel, movie, 196, 5)

    bestModel.predict(196, 464)

    RecommendUsers(bestModel, movie, 464, 5)


  }

  def prepareTrainData(ratingRDD: RDD[Rating]): (RDD[Rating], RDD[Rating], RDD[Rating]) = {
    val Array(trainData, validationData, testData) = ratingRDD.randomSplit(Array(0.8, 0.1, 0.1))
    (trainData, validationData, testData)
  }

  def prepareData(userDataPath: String, itemDataPath: String): (RDD[Rating], Map[Int, String]) = {
    val conf = new SparkConf().setMaster("local").setAppName("Movie")
    val sc = new SparkContext(conf)
    val userData = sc.textFile(userDataPath)
    sc.setLogLevel("ERROR")
    val rawRatings = userData.map(_.split('\t').slice(0, 3))

    val ratingRDD = rawRatings.map{
      case  Array(user, movie, rating) => Rating(user.toInt, movie.toInt, rating.toDouble)
    }

    val itemRDD = sc.textFile(itemDataPath)
    val movie = itemRDD.map(_.split("\\|"))
      .map(x => (x(0).toInt, x(1))).collectAsMap

    (ratingRDD, movie)

  }

  def recommendMovies(model:MatrixFactorizationModel, movieTitle: Map[Int, String], inputUserID: Int, num: Int): Unit ={
    val RecommendMovie = model.recommendProducts(inputUserID, num).mkString("\n")
    println(RecommendMovie)
  }
  def RecommendUsers(model: MatrixFactorizationModel, movieTitle: Map[Int, String], inputMovieID: Int, num: Int): Unit ={
    val RecomendUser = model.recommendUsers(inputMovieID, num).mkString("\n")
    println(RecomendUser)
  }

  def computeRmse(model: MatrixFactorizationModel, validationData: RDD[Rating]): Double = {
    val num = validationData.count
    val predictedRDD = model.predict(validationData.map(x => (x.user, x.product)))
    val predictedAndRatings = predictedRDD.map(p => ((p.user, p.product), p.rating)).join(validationData.map(x => ((x.user, x.product), x.rating))).values
    math.sqrt(predictedAndRatings.map(x => (x._1 - x._2) * (x._1 - x._2)).reduce(_ + _) / num)
  }

  def trainModel(trainData: RDD[Rating], validationData: RDD[Rating], rank: Int, it: Int, lambda: Double): (MatrixFactorizationModel, Double) = {
    val model = ALS.train(trainData, rank, it, lambda)
    val rmse = computeRmse(model, validationData)
    (model, rmse)
  }

  def evaluateALLParameter(trainData: RDD[Rating], validationData: RDD[Rating], ranks: Array[Int], its: Array[Int], lamdas: Array[Double]): (MatrixFactorizationModel, Double, String) = {
    var bestrmse = Double.MaxValue
    var pData = ""
    var bestModel: MatrixFactorizationModel = null
    for (rank <- ranks; it <- its; lamda <- lamdas) {
      val (model, rmse) = trainModel(trainData, validationData, rank, it, lamda)
      if (rmse < bestrmse) {
        bestrmse = rmse
        bestModel = model
        pData = "rank:" + rank+" it:" + it+" lamda:" + lamda
      }
    }
    (bestModel , bestrmse, pData)
  }


  def trainValidation(trainData: RDD[Rating], validationData: RDD[Rating]): (MatrixFactorizationModel, Double, String) = {
    evaluateALLParameter(
      trainData,
      validationData,
      Array(5, 10, 15, 20, 50, 100), Array(5, 10, 15, 20, 25), Array(0.05, 0.1, 1,5, 10.0)
    )
  }
}
