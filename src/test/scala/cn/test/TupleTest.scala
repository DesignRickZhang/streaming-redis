package cn.test

object TupleTest {
  def main(args: Array[String]): Unit = {
    val products = Seq("iPhone cover"-> 9.99, "Headphones"->5.49, "Samesung Galaxy cover"->8.95,
        ("ipad cover",7.49))
    println(products)   
      println(products(0)._1)   
           println(products(products.length-1)._1)   
  }
}