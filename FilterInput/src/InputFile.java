
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.*;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.KeyValueTextInputFormat;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class InputFile extends Configured implements Tool {

  static String[] checkWords;
  static double d = 0.85;
  //static int N = 679773; 
  //True N
  static int N = 685230;
  //static int N = 7;
  //compute filter parameters for netid ms2786
  static double fromNetID = 0.6872;
  static double rejectMin = 0.99 * fromNetID;
  static double rejectLimit = rejectMin + 0.01;
  static enum RecordCounters{ RESIDUAL_COUNTER };
  //assume 0.0 <= rejectMin < rejectLimit <= 1.0

  /**
   * is a function that returns the blockID of a node given the NodeID
   * @param NodeID
   * @return blockID
   */
  public int run(String[] args) throws Exception {
	  if(args.length < 2){
		  return -1;
	  }
  	
	  checkWords = new String[args.length-2];

	  int numIter = 5;

	  Path input = new Path(args[0]);

	  for(int i = 0; i < numIter; i++){
		  JobConf conf = new JobConf(getConf(), InputFile.class);
		  conf.setJobName("indexwords");

		  conf.setInputFormat(KeyValueTextInputFormat.class);
		  conf.setOutputFormat(TextOutputFormat.class);

		  conf.setOutputKeyClass(Text.class);
		  conf.setOutputValueClass(Text.class);

//		  conf.setMapperClass(MapClass.class);
	//	  conf.setReducerClass(Reduce.class);

		  FileInputFormat.setInputPaths(conf, input);
		  FileOutputFormat.setOutputPath(conf, new Path(args[1] + Integer.toString(i)));

		  RunningJob rj = JobClient.runJob(conf);
		  input = new Path(args[1]+ Integer.toString(i));
		  double resVal = rj.getCounters().getCounter(RecordCounters.RESIDUAL_COUNTER) * 1.0/10000;
		  System.out.println(N+" "+(resVal/(1.0*N)));
		  if(resVal/(1.0*N) < 0.001) break;
	  }

	  return 0;
  }


  public static boolean selectInputLine(double x) {
		return ( ((x >= rejectMin) && (x < rejectLimit)) ? false : true );
  }
}