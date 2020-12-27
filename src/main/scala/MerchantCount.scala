import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

object MerchantCount {
   def main(args: Array[String]) {
     if (args.length !=3) {
       System.err.println("Usage: <input_log> <input_info> <output>")
       System.exit(1)
     }
     val conf = new SparkConf().setAppName("Favorite Merchant For Youngsters")
     val sc = new SparkContext(conf)
     val log = sc.textFile(args(0))
     //先从log中筛选出日期是1111且行为是1,2,3的
     val ageAll=log.filter(line=>{
       val splitLine=line.split(",")
       splitLine(5)=="1111"&&splitLine(6).matches("[123]")
     })
     //从info中找出年龄不是1,2,3的
     //注意！info文件要处理缺失值，这里直接丢弃缺失值
     val excludeUserId=sc.textFile(args(1)).filter(_.split(",").length==3).filter(_.split(",")(1).matches("[^123]")).map(_.split(",")(0))
     //利用RDD[(user_id,merchant_id)]，从ageAll的user_id部分中去掉excludeUserId，得到符合要求的merchanti_id
     val MerchantForYoung=ageAll.map(line=>(line.split(",")(0),line.split(",")(3))).subtractByKey[Int](excludeUserId.map[(String,Int)]((_,1))).values
     //计数并排序,取前100个
     val count=MerchantForYoung.map((_,1)).reduceByKey(_+_).map(tuple=>(tuple._2,tuple._1)).sortByKey(ascending = false).take(100).map(tuple=>(tuple._2,tuple._1))
     //按格式输出
     val result=count.map(tuple=>"商家id:"+tuple._1+",次数:"+tuple._2.toString)
     sc.parallelize(result).saveAsTextFile(args(2))

     sc.stop()
   }
}
