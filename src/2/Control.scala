object Control {
  def main(args: Array[String]) {
    // if
    val x = 3
    if (x > 0) {
      // do something
    } else {
      // do some thing
    }

    val a: Int = if (x > 0) 1 else -1

    // while
    var i: Int = 9
    while (i > 0) {
      i -= 1
    }

    // do
    i = 0
    do {
      i += 1
    } while (i < 5)

    // for
    for (i <- 1 to 5 if i % 2 == 0) println(i)
    for (i <- 1 to 5; j <- 1 to 3) println(i * j)

    val r: Array[Int] = for (i <- Array(1, 2, 3, 4, 5) if i % 2 == 0) yield {
        println(i); i
    }

    // break
    import util.control.Breaks._
    val array: Array[Int] = Array(1, 3, 10, 5, 4)
    breakable {
      for (i <- array) {
        if (i > 5) break
        println(i)
      }
    }
    // print out 1 3


    for (i <- array) {
      breakable {
        if (i > 5) break
        println(i)
      }
    }
    // print out 1 3 5 4
  }

}
