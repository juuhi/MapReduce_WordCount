package com.bigdata;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCount_Mapper extends Mapper<LongWritable, Text, Text, IntWritable>{
	
	@Override
	public void map(LongWritable inputKey, Text inputValue, Context context) throws IOException, InterruptedException{
		
		String input_value = inputValue.toString().replaceAll("[^a-zA-Z\\s]", "");
		String[] words = input_value.split(" ");
		
		for(int i = 0; i < words.length; i++) {
			context.write(new Text(words[i]), new IntWritable(1)); 
		}
		
	}
	
}
