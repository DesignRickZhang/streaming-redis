package cn.my.curator

import org.apache.curator.framework.CuratorFramework

object CuratorUtilTest {
  def main(args: Array[String]): Unit = {
    var bread  = "/hadoop-ha/ns/ActiveBreadCrumb"
     val   path = "/hadoop-ha/ns"
     
     val  PATH = "/crud";  
       val zk: CuratorFramework = SparkCuratorUtil.newClient("vm10:2181")
      val result =  zk.getChildren.forPath(path)
      println(result)
       val result2 =  zk.getChildren.forPath(path)
//       
//              val  bs2 = zk.getData().watched().inBackground().forPath(path);  
//            System.out.println("修改后的data为" + new String(bs2 != null ? bs2 : new byte[0]));  
       val bread2 = zk.getData().forPath(bread)
       println(new String(bread2,"utf8"))
       
       
//             zk.create().forPath(PATH, "I love messi".getBytes());  
  
            val bs = zk.getData().forPath(PATH);  
            System.out.println("新建的节点，data为:" + new String(bs));  
            
                val  bs2 = zk.getData().watched().inBackground().forPath(PATH);  
            val v =  new Array[Byte](0)
            System.out.println("修改后的data为" + new String(if(bs2!=null) bs2 else v));  
  
  }
}