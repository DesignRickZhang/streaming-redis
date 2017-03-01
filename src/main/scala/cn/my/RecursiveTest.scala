package cn.my

object RecursiveTest {
  def main(args: Array[String]): Unit = {
    
    def sum(xs:List[Int]) :Int  = xs match{
      case Nil => println("nil");0; 
      case x:: ys =>  
//        println("x: "+x+", ys:"+ ys); 
        x + sum(ys)
    }
//    println((1 to 3).toList)
    println(sum((1 to 10000).toList))
//    println(sum(List(1,2,3)))
  }
}