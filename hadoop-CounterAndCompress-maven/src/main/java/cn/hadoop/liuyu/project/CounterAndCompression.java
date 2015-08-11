package cn.hadoop.liuyu.project;

import java.net.URI;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


//计数器和压缩算法的使用

/**
 * @function 统计无效数据和对输出结果进行压缩
 */
public class CounterAndCompression extends Configured implements Tool {
	// 定义枚举对象
	public static enum LOG_PROCESSOR_COUNTER {
		BAD_RECORDS
	};
	
	/**
	 * @function Mapper 解析数据，统计无效数据，并输出有效数据
	 */
	public static class CounterAndCompressionMapper extends Mapper<LongWritable, Text, Text, Text> {
		protected void map(LongWritable key, Text value, Context context) throws java.io.IOException, InterruptedException {
			// 解析每条机顶盒记录，返回list集合
			List<String> list = ParseTVData.transData(value.toString());
			int length = list.size();//获得list集合的元素的个数
			// 无效记录
			if (length == 0) {
				// 动态自定义计数器
				context.getCounter("ErrorRecordCounter", "ERROR_Record_TVData").increment(1);
				// 枚举声明计数器
				context.getCounter(LOG_PROCESSOR_COUNTER.BAD_RECORDS).increment(1);
			} else {
				for (String validateRecord : list) {
					//输出解析数据
					context.write(new Text(validateRecord), new Text(""));
				}
			}
		}
	}
	
	/**
	 * @function 任务驱动方法
	 * 
	 */
	public int run(String[] args) throws Exception {
		//读取配置文件
		Configuration conf = new Configuration();
		//文件系统接口
		URI uri = new URI("hdfs://master:9000");
		//输出路径
		Path mypath = new Path(args[1]);
		// 创建FileSystem对象
		FileSystem hdfs = FileSystem.get(uri, conf);
		if (hdfs.isDirectory(mypath)) {
			//删除已经存在的文件路径
			hdfs.delete(mypath, true);
		}
		
		Job job = Job.getInstance(conf);//新建一个任务
		job.setJarByClass(CounterAndCompression.class);//设置主类
		
		job.setMapperClass(CounterAndCompressionMapper.class);//只有 Mapper
		job.setOutputKeyClass(Text.class);//输出 key 类型
		job.setOutputValueClass(Text.class);//输出 value 类型
		
		FileInputFormat.addInputPath(job, new Path(args[0]));//输入路径
		FileOutputFormat.setOutputPath(job, new Path(args[1]));//输出路径
		
		FileOutputFormat.setCompressOutput(job, true);//对输出结果设置压缩
		FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);//设置压缩类型
		
		job.waitForCompletion(true);//提交任务
		return 0;
	}
	
	/**
	 * @function main 方法
	 * @param args 输入    输出路径
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {	
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();    //解析命令行参数
		if (otherArgs.length != 2) {  //判断命令行的参数是否为两个
            System.err.println("Usage: MyCounter < in> < out>");  
            System.exit(2);  
        }  
        
		String[] date = {"20120917","20120918","20120919","20120920","20120921","20120922","20120923"};
		int ec = 1;
		for(String dt:date){
			String[] args0 = { args[0]+dt+".txt",
			args[1]+dt };
			ec = ToolRunner.run(new Configuration(), new CounterAndCompression(), args0);
		}		
		System.exit(ec);
	}
}

