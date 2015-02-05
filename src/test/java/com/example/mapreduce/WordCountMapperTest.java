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
import org.apache.hadoop.mrunit.MapDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;


/**
 * The Class WordCountMapperTest.
 */
public class WordCountMapperTest {

	/** The test mapper. */
	private Mapper<LongWritable, Text, Text, IntWritable> testMapper;

	/** The test driver. */
	private MapDriver<LongWritable, Text, Text, IntWritable> testDriver;


	/**
	 * Setup.
	 */
	@Before
	public void setup() {
		testMapper = new WordCountMapper();
		testDriver = MapDriver.newMapDriver(testMapper);
	}


	/**
	 * Test word count.
	 * 
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	@Test
	public void testWordCount() throws IOException {
		String input = "The quick brown fox jumps over the lazy dog!";

		List<Pair<Text, IntWritable>> results = testDriver.withInput(new LongWritable(1), new Text(input)).run();
		boolean found = false;
		for (Pair<Text, IntWritable> p : results) {
			if (p.getFirst().toString().equalsIgnoreCase("quick") && p.getSecond().get() == 1) {
				found = true;
			}
		}
		assertEquals(found, true);
	}
}
