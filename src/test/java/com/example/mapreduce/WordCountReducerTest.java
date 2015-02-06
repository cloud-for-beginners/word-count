/*
 * (c) Cloud for Beginners.
 * 
 * author: tmusabbir
 */
package com.example.mapreduce;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mrunit.MapReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;


/**
 * The Class WordCountReducerTest.
 */
public class WordCountReducerTest {

	/** The test mapper. */
	private Mapper<LongWritable, Text, Text, IntWritable> testMapper;

	/** The test reducer. */
	private Reducer<Text, IntWritable, Text, IntWritable> testReducer;

	/** The test driver. */
	private MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable> testDriver;


	/**
	 * Setup.
	 */
	@Before
	public void setup() {
		testMapper = new WordCountMapper();
		testReducer = new WordCountReducer();
		testDriver = MapReduceDriver.newMapReduceDriver(testMapper, testReducer);
	}


	/**
	 * Test word count.
	 *
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	@Test
	public void testWordCount() throws IOException {
		String input1 = "the quick brown fox jumps over the lazy dog!";
		Pair<LongWritable, Text> pair = new Pair<LongWritable, Text>(new LongWritable(1), new Text(input1));
		List<Pair<Text, IntWritable>> results = testDriver.withInput(pair).run();

		for (Pair<Text, IntWritable> p : results) {
			if (p.getFirst().toString().equalsIgnoreCase("the")) {
				assertEquals(p.getSecond().get(), 2);
			}
		}
	}
}
