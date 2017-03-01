package cn.test

object TraitTest extends B with User with App{
  override def name = "apple"
  println(this)
}
trait User{def name:String}
trait B{
  self:User =>
    {println("user:User")}
  def foo(){
    println(name)
  }  
}