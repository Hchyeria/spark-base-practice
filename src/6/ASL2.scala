import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


object ASL {
  def main(args: Array[String]): Unit = {
         
    System.setProperty("hadoop.home.dir", "E:\\hadoop\\hadoop-2.6.0")
    //u.data(userid  itemid  rating  timestamp)
    //u.item(主要使用 movieid movietitle)

    var userData="src/data/u.data"
    var itemData="src/data/u.item"
    val conf = new SparkConf().setMaster("local").setAppName("myti")
    val sc = new SparkContext(conf)
    val userdata=sc.textFile(userData)
    val rawRatings=userdata.map(_.split('\t').take(3))
    //   rawRatings.take(2)(0).foreach(println(_))
    val ratingrdd=rawRatings.map{case Array(user,movie,rating)=>Rating(user.toInt,movie.toInt,rating.toDouble)}
//    ratingRDD.foreach(println) 
    val itemrdd=sc.textFile(itemData)
    val moviet=itemrdd.map(_.split("\\|")).map(x=>(x(0).toInt,x(1))).collectAsMap
//        moviet.foreach(println)
//    (ratingRDD,moviet)
    val Array(trainData,validationData,testData)=ratingrdd.randomSplit(Array(0.8,0.1,0.1))
    //1.trainModel
    //val model=ALS.train(trainData,rank,it,lambda)
    //Rating(userID,productID,rating),rank 当矩阵分解matrix factorization时，将原本矩阵A(mXn)分解成X(m X rank) Y(rank X n)矩阵
    //Iterator   ALS算法重复计算次数，建议10~20
    //lambda   建议0.01  会影响准确度|时间
    //MatrixFactorizationMode训练完成后产生的模型，训练时会执行矩阵分解。模型对象成员 rank，

    val model=ALS.train(trainData,10,10,0.01)
    //1.针对用户 推荐电影 
    var movies=model.recommendProducts(196,5) //.mkString("\n")
//    movies.map(f)
    println("推荐电影 显示前5条推荐电影名称 "+movies)
    //2 显示前5条推荐电影名称    查询电影名称  moviet(590)

   movies.map(rating=>(rating.product,moviet(rating.product),rating.rating))
   .foreach(println)

    //3.针对电影 推荐给用户
// model.recommendUsers(product:Int,num:Int):Array[Rating] 方法推荐
//product要被推荐的电影ID，num推荐的记录数，返回系统针对产品推荐给用户的数组

      model.recommendUsers(464,5).mkString(",")

    
//    val num = validationData.count
//    val predictedRDD = model.predict(validationData.map(x=>(x.user,x.product)))
//    val predictedAndRatings = predictedRDD.map(p=>((p.user,p.product),p.rating)).join(validationData.map(x=>((x.user,x.product),x.rating))).values
//    var accuracy=math.sqrt(predictedAndRatings.map(x=>(x._1-x._2)*(x._1-x._2)).reduce(_+_)/num)
//    println("accuracy="+accuracy)
     
    print("End......")
  }

  def prepareData1(userData:String,itemData:String): Unit ={
    val conf = new SparkConf().setMaster("local").setAppName("myti")
    val sc = new SparkContext(conf)
    val userdata=sc.textFile(userData)
    val rawRatings=userdata.map(_.split('\t').take(3))
    //   rawRatings.take(2)(0).foreach(println(_))
    val ratingRDD=rawRatings.map{case Array(user,movie,rating)=>Rating(user.toInt,movie.toInt,rating.toDouble)}
//    ratingRDD.foreach(println) 
    val itemrdd=sc.textFile(itemData)
    val moviet=itemrdd.map(_.split("\\|").take(2)).map(x=>(x(0).toInt,x(1))).collectAsMap
//    moviet.foreach(println)
    (ratingRDD,moviet)
    
    prepareData2(ratingRDD)
  }
  def prepareData2(ratingrdd:RDD[Rating]):(RDD[Rating],RDD[Rating],RDD[Rating])={
    val Array(trainData,validationData,testData)=ratingrdd.randomSplit(Array(0.8,0.1,0.1))
    return (trainData,validationData,testData)
  }
  def recommendMovies(model:MatrixFactorizationModel,movieTitle:Map[Int, String],inputUserID:Int)={
    val RecommendMovie=model.recommendProducts(inputUserID,10)
    var i=1
    RecommendMovie.foreach{r=>println(i.toString+movieTitle(r.product)+r.rating);i+=1}
  }
  def RecommendUsers(model:MatrixFactorizationModel,movieTitle:Map[Int,String],inputMovieID:Int)={
    val RecomendUser=model.recommendUsers(inputMovieID,10)
    var i=1
    RecomendUser.foreach{r=>println(i.toString+r.user+r.rating); i+=1 }
  }

  def evaluateParameter(trainData:RDD[Rating], validationData:RDD[Rating],p:String,ranks:Array[Int],its:Array[Int],lamdas:Array[Double]):(Double,String)= {

    var bestrmse=Double.MaxValue
    var pData =""
    for (rank <- ranks; it <- its; lamda <- lamdas) {
      val rmse = trainModel(trainData, validationData, rank, it, lamda)
      if(rmse<bestrmse){
        bestrmse=rmse
        pData = p match {
          case "rank" => "rank:" + rank;
          case "it" => "it:" + it;
          case "lamda" => "lamda:" + lamda;
        }
      }
    }
    (bestrmse, pData)
  }
  def evaluateALLParameter(trainData:RDD[Rating], validationData:RDD[Rating],ranks:Array[Int],its:Array[Int],lamdas:Array[Double]):(Double,String)= {

    var bestrmse=Double.MaxValue
    var pData =""
    for (rank <- ranks; it <- its; lamda <- lamdas) {
      val rmse = trainModel(trainData, validationData, rank, it, lamda)
      if(rmse<bestrmse){
        bestrmse=rmse
        pData = "rank:" + rank+" it:" + it+" lamda:" + lamda
      }
    }

    (bestrmse, pData)
  }

  def trainValidation(trainData:RDD[Rating], validationData:RDD[Rating]): (Double,String) =
  {
    val bestModel=evaluateALLParameter(trainData,validationData, Array(5,10,15,20,50,100), Array(5,10,15,20,25), Array(0.05,0.1,1,5,10.0))
    return (bestModel)
  }
  def trainModel(trainData:RDD[Rating], validationData:RDD[Rating],rank:Int,it:Int,lambda:Double):Double={
    val model=ALS.train(trainData,rank,it,lambda)
    val rmse=computeRmse(model,validationData)
    (rmse)
  }
  def computeRmse(model:MatrixFactorizationModel,validationData:RDD[Rating]):Double =
  {
    val num = validationData.count
    val predictedRDD = model.predict(validationData.map(x=>(x.user,x.product)))
    val predictedAndRatings = predictedRDD.map(p=>((p.user,p.product),p.rating)).join(validationData.map(x=>((x.user,x.product),x.rating))).values
    return(math.sqrt(predictedAndRatings.map(x=>(x._1-x._2)*(x._1-x._2)).reduce(_+_)/num))
  }

}