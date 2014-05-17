


import java.io.IOException;
import java.net.URI;
import java.util.*;
import java.util.regex.*;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class WordCount {
	public static Pattern pattern,pattern_word,pattern_datetime;
  public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    private int [] TIME_START={4,21,19,43};
    //private int [] TIME_END={5,1,11,40};
    //private int [] TIME_RANGE={0,9,15,57};
    private long TWEET_PER_HOUR=(long)(4687787439L/232);
    private String keyToTime(Long key){
    	double total_hour=key/(double)(TWEET_PER_HOUR);
    	int minute=(int) (Math.floor(60*total_hour));//-Math.floor(total_hour)));
    	//hour=((int) Math.floor(total_hour))%24;
    	//day=(int)(Math.floor(total_hour/24));
    	int []time_now=TIME_START.clone();
    	time_now[3]+=minute;
    	if (time_now[3]>=60){
    		time_now[2]+=(int)time_now[3]/60;
    		time_now[3]=time_now[3]%60;
    	}
       	if (time_now[2]>=24){
    		time_now[1]+=(int)time_now[2]/24;
    		time_now[2]=time_now[2]%24;
    	}
       	if (time_now[1]>30){
       		time_now[0]+=(int)time_now[1]/30;
    		time_now[1]=time_now[1]%30;
       	}
       	return time_now[0]+" "+(time_now[1]<10?"0":"")+time_now[1]+" "+(time_now[2]<10?"0":"")+time_now[2];//+" "+(time_now[3]<10?"0":"")+time_now[3];
    }
    /*public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
      String line = value.toString();
      Matcher matcher=pattern.matcher(line);
      
      if (matcher.matches()){
    	  String datetime,tweet,datehour;
    	  datetime=matcher.group(1);
    	  Matcher matcher_datehour=pattern_datetime.matcher(datetime);
    	  if (matcher_datehour.matches()){
    		  datehour=matcher_datehour.group(1)+matcher_datehour.group(2);
        	  tweet=matcher.group(2);
        	  Matcher matcher_word=pattern_word.matcher(tweet);
              while (matcher_word.find()) {
                word.set(datehour+' '+matcher_word.group(1));
                output.collect(word, one);
              }
    	  }
      }
    }*/
    public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
        try {
        	pattern=Pattern.compile("@(.*?) - (.*?)$");
        	pattern_word=Pattern.compile("#([\\w&&[^0-9]].*?)\\b");
        	pattern_datetime=Pattern.compile("(.*? .*? )(\\d\\d):\\d\\d:\\d\\d .*$");
			String line = value.toString();
			String datehour=keyToTime(key.get());
			Matcher matcher=pattern.matcher(line);
			
			if (matcher.matches()){
			  String tweet;
			  tweet=matcher.group(2);
			  
			  Matcher matcher_word=pattern_word.matcher(tweet);
			    while (matcher_word.find()) {
			      word.set(datehour+' '+matcher_word.group(1));
			      output.collect(word, one);
			    }
			  
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
      }
  }

  public static class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
    public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
      try{
    	  int sum = 0;
	      while (values.hasNext()) {
	        sum += values.next().get();
	      }
	      output.collect(key, new IntWritable(sum));
		}
		catch(Exception e){
			e.printStackTrace();
		}
    }
  }

  public static void main(String[] args) throws Exception {
	//pattern=Pattern.compile("(.*?)\t(.*?)$");
	pattern=Pattern.compile("@(.*?) - (.*?)$");
	pattern_word=Pattern.compile("#([\\w&&[^0-9]].*?)\\b");
	pattern_datetime=Pattern.compile("(.*? .*? )(\\d\\d):\\d\\d:\\d\\d .*$");
	try{
		//args=new String[2];
		//args[0]="input";
		//args[1]="output";
	    JobConf conf = new JobConf(WordCount.class);
	    conf.setJobName("wordcount");
	    conf.set("mapreduce.output.textoutputformat.separator", "\t");
	    conf.setOutputKeyClass(Text.class);
	    conf.setOutputValueClass(IntWritable.class);
	
	    conf.setMapperClass(Map.class);
	    conf.setCombinerClass(Reduce.class);
	    conf.setReducerClass(Reduce.class);
	
	    conf.setInputFormat(TextInputFormat.class);
	    conf.setOutputFormat(TextOutputFormat.class);
	    //conf.setOutputFormat(Text.class);
	    URI uri_input=URI.create(args[0]),uri_output=URI.create(args[1]);
	    FileInputFormat.setInputPaths(conf, new Path(uri_input));
	    FileOutputFormat.setOutputPath(conf, new Path(uri_output));
	    //FileSystem.getLocal(conf).delete(new Path(args[1]), true);
	    JobClient.runJob(conf);
	}
	catch(Exception e){
		System.out.println(e);
	}
  }
}


