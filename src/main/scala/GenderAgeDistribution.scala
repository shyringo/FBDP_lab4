import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

object GenderAgeDistribution {
   def main(args: Array[String]) {
     if (args.length !=3) {
       System.err.println("Usage: <input_log> <input_info> <output>")
       System.exit(1)
     }
     val conf = new SparkConf().setAppName("Gender and age distribution")
     val sc = new SparkContext(conf)
     val log = sc.textFile(args(0))
     //先从log中筛选出日期是1111且行为是2的用户id，并去重
     val allAgeGender=log.filter(line=>{
       val splitLine=line.split(",")
       splitLine(5)=="1111"&&splitLine(6)=="2"
     }).map(_.split(",")(0)).distinct()
     //从info中筛选user_id部分包含allAgeGender的line
     //注意！info文件要处理缺失值，这里直接丢弃缺失值;且丢弃异常值
     val info=sc.textFile(args(1)).filter(line=>{
       val splitLine=line.split(",")
       splitLine.length==3&&splitLine(1)!=""&&splitLine(1)!="0"&&splitLine(2)!="2"
     }).map(line=>(line.split(",")(0),line)).join(allAgeGender.map((_,1))).map(_._2._1)
     //利用countByValue计算频率，并除以总长度len来计算比例
     val len=info.count()
     val genders=info.map(_.split(",")(2)).countByValue().toList
     val ages=info.map(_.split(",")(1)).countByValue().toList
     //将两个结果的rdd相union为一个，并按自定义格式输出
     //注意转换为Double类型除法
     sc.parallelize(genders).map(tuple=>"性别:"+tuple._1+",比例:"+(tuple._2.toDouble/len).toString).union(sc.parallelize(ages).map(tuple=>"年龄段:"+tuple._1+",比例:"+(tuple._2.toDouble/len).toString)).saveAsTextFile(args(2))

     sc.stop()
   }
}
