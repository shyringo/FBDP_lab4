import java.io.*;
import java.nio.charset.StandardCharsets;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.map.InverseMapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.io.WritableComparable;

public class GoodsCount {

  public static class FilterMapper
          extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    private final Text word = new Text();

    public void map(Object key, Text value, Context context
    ) throws IOException, InterruptedException {
      String line = value.toString() ;
      String[] splitLine=line.split(",");
      //如果日期是1111且行为是1,2,3中的一种，则对商品id进行计数
      if(splitLine[5].equals("1111")&&splitLine[6].matches("[123]")){
        word.set(splitLine[1]);
        context.write(word, one);
      }
    }
  }

  public static class IntSumReducer
          extends Reducer<Text,IntWritable,Text,IntWritable> {
    private final IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
    ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }

  //自定义outputformat
  public static  class RankOutputFormat<K,V> extends TextOutputFormat<K,V>{
    public RecordWriter<K, V> getRecordWriter(TaskAttemptContext job) throws IOException, InterruptedException {
      Configuration conf = job.getConfiguration();
      boolean isCompressed = getCompressOutput(job);
      String keyValueSeparator = "\t";
      CompressionCodec codec = null;
      String extension = "";
      if (isCompressed) {
        Class<? extends CompressionCodec> codecClass = getOutputCompressorClass(job, GzipCodec.class);
        codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, conf);
        extension = codec.getDefaultExtension();
      }

      Path file = this.getDefaultWorkFile(job, extension);
      FileSystem fs = file.getFileSystem(conf);
      FSDataOutputStream fileOut;
      if (!isCompressed) {
        fileOut = fs.create(file, false);
        return new RankOutputFormat.LineRecordWriter(fileOut, keyValueSeparator);
      } else {
        fileOut = fs.create(file, false);
        return new RankOutputFormat.LineRecordWriter(new DataOutputStream(codec.createOutputStream(fileOut)), keyValueSeparator);
      }
    }

    protected static class LineRecordWriter<K, V> extends RecordWriter<K, V> {
      private static final byte[] NEWLINE;
      protected DataOutputStream out;

      public LineRecordWriter(DataOutputStream out, String keyValueSeparator) {
        this.out = out;
      }

      private void writeObject(Object o) throws IOException {
        if (o instanceof Text) {
          Text to = (Text)o;
          this.out.write(to.getBytes(), 0, to.getLength());
        } else {
          this.out.write(o.toString().getBytes(StandardCharsets.UTF_8));
        }

      }
      //直接用一个数字计数，表示名次
      public  static IntWritable rank=new IntWritable(1);
      //此函数写具体格式
      public synchronized void write(K key, V value) throws IOException {
        boolean nullKey = key == null || key instanceof NullWritable;
        boolean nullValue = value == null || value instanceof NullWritable;
        if ((!nullKey || !nullValue)&&rank.compareTo(new IntWritable(100))<=0) {
          //输出排名:
          this.writeObject(rank);
          this.writeObject(":");
          //以下输出单词,次数
          if (!nullValue) {
            this.writeObject(value);
          }
          if (!nullKey && !nullValue) {
            this.writeObject(",");
          }
          if (!nullKey) {
            this.writeObject(key);
          }

          this.out.write(NEWLINE);
        }
        rank.set(rank.get()+1);
      }

      public synchronized void close(TaskAttemptContext context) throws IOException {
        this.out.close();
      }

      static {
        NEWLINE = "\n".getBytes(StandardCharsets.UTF_8);
      }
    }

  }

  //自定义comparator改为降序（默认升序）
  private static class IntWritableDecreasingComparator extends IntWritable.Comparator {
    public int compare(WritableComparable a, WritableComparable b) {
      return -super.compare(a, b);
    }

    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
      return -super.compare(b1, s1, l1, b2, s2, l2);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    GenericOptionsParser optionParser = new GenericOptionsParser(conf, args);
    String[] remainingArgs = optionParser.getRemainingArgs();
    if (remainingArgs.length!=2) {
      System.err.println("Usage: WordCount <in> <out>");
      System.exit(2);
    }
    //job1:goods count
    Job job = Job.getInstance(conf, "goods count");
    job.setJarByClass(GoodsCount.class);
    job.setMapperClass(FilterMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    //自定义outputformat以按特定格式输出结果
    job.setOutputFormatClass(SequenceFileOutputFormat.class);
    FileInputFormat.addInputPath(job, new Path(remainingArgs[0]));
    //建立中转文件夹，将job1的结果转给job2
    Path midRes=new Path("midRes");
    FileOutputFormat.setOutputPath(job,midRes);

    //job2: sort desc
    if(job.waitForCompletion(true))
    {
      Job sortJob =Job.getInstance(conf, "sort desc");
      sortJob.setJarByClass(GoodsCount.class);
      FileInputFormat.addInputPath(sortJob, midRes);
      sortJob.setInputFormatClass(SequenceFileInputFormat.class);
      /*InverseMapper由hadoop库提供，作用是实现map()之后的数据对的key和value交换*/
      sortJob.setMapperClass(InverseMapper.class);
      FileOutputFormat.setOutputPath(sortJob, new Path(remainingArgs[1]));
      sortJob.setOutputKeyClass(IntWritable.class);
      sortJob.setOutputValueClass(Text.class);
      sortJob.setOutputFormatClass(RankOutputFormat.class);
      //IntWritableDecreasingComparator是一个降序comparator（默认升序）
      sortJob.setSortComparatorClass(IntWritableDecreasingComparator.class);
      if(sortJob.waitForCompletion(true)){
        //最后把中转文件夹删掉
        FileSystem.get(conf).deleteOnExit(midRes);
        System.exit( 0 );
      }
      else{
        System.out.println("job2 failed\n");
        System.exit(1);
      }
    }
    else{
      System.out.println("job1 failed");
      System.exit(1);
    }
  }
}