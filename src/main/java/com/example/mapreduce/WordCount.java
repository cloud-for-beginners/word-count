/*
 * (c) Cloud for Beginners.
 */
package com.example.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Log4jConfigurer;


/**
 * The Class WordCount.
 */
public class WordCount extends Configured implements Tool {

	/** The Constant LOGGER. */
	private static final Logger LOGGER = LoggerFactory.getLogger(WordCount.class);


	/* (non-Javadoc)
	 * @see org.apache.hadoop.util.Tool#run(java.lang.String[])
	 */
	public int run(String[] args) throws Exception {
		if (args.length != 2) {
			LOGGER.error("Usage: word-count-0.0.1.jar [generic options] <input> <output>");
			ToolRunner.printGenericCommandUsage(System.err);
			return -1;
		}

		int ret = 0;
		LOGGER.info("Starting word count job...");

		Path inputPath = new Path(args[0]);
		Path outputPath = new Path(args[1]);
		Configuration hadoopConf = getConf();
		JobConf job = new JobConf(hadoopConf, WordCount.class);
		job.setJarByClass(getClass());
		job.setJobName("Word Count");

		FileInputFormat.setInputPaths(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);

		job.setInputFormat(TextInputFormat.class);
		job.setOutputFormat(TextOutputFormat.class);

		job.setMapperClass(WordCountMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setReducerClass(WordCountReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.setCombinerClass(WordCountReducer.class);

		JobClient.runJob(job);
		return ret;
	}


	/**
	 * The main method.
	 * 
	 * @param args the arguments
	 * @throws Exception the exception
	 */
	public static void main(String[] args) throws Exception {
		Log4jConfigurer.initLogging("classpath:META-INF/log4j.properties");
		WordCount wc = new WordCount();
		if (args != null && args.length > 0) {
			int exitCode = ToolRunner.run(wc, args);
			System.exit(exitCode);
		} else {
			throw new Exception("Usage: com.example.mapreduce.WordCount -input [path] -output[path]");
		}
	}
}
