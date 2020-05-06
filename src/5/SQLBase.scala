import org.apache.spark.sql.SparkSession

case class Emp(eid: Integer, ename: String, job: String, mgr: Integer,
               hiredate: String, sal: Double, bonus: Double, did: Integer)

case class Dept(did: Integer, dname: String, loc: String)


object SQLBase {
  def main(args: Array[String]) {
    val ss = SparkSession.builder()
      .master("local").appName("SQLBase").getOrCreate()
    ss.sparkContext.setLogLevel("ERROR")

    import ss.implicits._

    var path = "./src/5/emp.txt"
    val empDF = ss.sparkContext.textFile(path)
      .map(x => x.split(","))
      .map(line => Emp(line(0).toInt, line(1), line(2),
        if (line(3).isEmpty) 0 else line(3).toInt, line(4), line(5).toDouble,
        if (line(6).isEmpty) 0.0 else line(6).toDouble, line(7).toInt
      )).toDF()

    path = "./src/5/dept.txt"
    val deptDF = ss.read.csv(path)
      .map(line => Dept(line(0).toString.toInt, line(1).toString, line(2).toString))
      .toDF()

    empDF.createOrReplaceTempView("emp")
    deptDF.createOrReplaceTempView("dept")
    
    println("=============== 1 ===============")
    empDF.groupBy("did").sum("sal").show()

    println("=============== 2 ===============")
    ss.sql("select count(*) as count, avg(sal) as avg from emp group by did").show()

    println("=============== 3 ===============")
    empDF.orderBy($"hiredate").limit(1).select("ename").show()

    println("=============== 4 ===============")
    empDF.join(deptDF, "did").groupBy("loc").sum("sal").show()

    println("=============== 5 ===============")
    ss.sql("select e1.ename ,e2.sal from emp e1 left join emp e2 on e1.mgr = e2.eid where e1.sal > e2.sal").show()

    println("=============== 6 ===============")
    ss.sql("select ename, sal from emp where sal > (select avg(sal) from emp)").show()

    println("=============== 7 ===============")
    empDF.filter("like(ename, 'J%')").join(deptDF, "did").select("ename", "dname").show()

    println("=============== 8 ===============")
    empDF.orderBy($"sal".desc).limit(3).select("ename", "sal").show()

    println("=============== 9 ===============")
    empDF.sort(($"sal" + $"bonus").desc).select($"ename", $"sal" + $"bonus").show()
  }
}
