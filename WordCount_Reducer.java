package com.bigdata;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCount_Reducer extends Reducer<Text, IntWritable, Text, IntWritable>{

	public void reduce(Text inputKey, Iterable<IntWritable> inputValue, Context context) throws IOException, InterruptedException{
		
		int sum = 0;
		for(IntWritable value: inputValue) {
			sum += value.get();
		}
		context.write(inputKey, new IntWritable(sum));
		
	}
	
}