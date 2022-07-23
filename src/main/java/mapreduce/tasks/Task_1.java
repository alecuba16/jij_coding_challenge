package mapreduce.tasks;

import mapreduce.JobMapReduce;
import mapreduce.Utils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;

public class Task_1 extends JobMapReduce {

	public static class Task_1_Map extends Mapper<LongWritable, Text, IntWritable, IntWritable> {

		public boolean isDateInRange(String currentDate, String filterDateStart, String filterDateEnd){
//			######################
//			###	YOUR CODE HERE ###
//			######################
			return false;
		}

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			String adIdColumn = context.getConfiguration().getStrings("adIdColumn")[0];
			String siteIdColumn = context.getConfiguration().getStrings("siteIdColumn")[0];
			String dateColumn = context.getConfiguration().getStrings("dateColumn")[0];

			String filterDateStart = context.getConfiguration().getStrings("filterDateStart")[0];
			String filterDateEnd = context.getConfiguration().getStrings("filterDateEnd")[0];

			String[] arrayValues = value.toString().split(",");

			String adIdValue = Utils.getAttributeSiteAds(arrayValues, adIdColumn);

//			######################
//			###	YOUR CODE HERE ###
//			######################

					IntWritable outputKey = new IntWritable(0);
					IntWritable outputValue = new IntWritable(0);

					context.write(outputKey,outputValue);

		}
	}

	public static class Task_1_Reduce extends Reducer<IntWritable, IntWritable, NullWritable, Text> {



		public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {


			for (IntWritable value : values) {
//				######################
//				###	YOUR CODE HERE ###
//				######################

			}

		}

	}

	public Task_1() {
		this.input = null;
		this.output = null;
	}

	public boolean run() throws IOException, ClassNotFoundException, InterruptedException {
		Configuration configuration = new Configuration();
		// Define the new job and the name it will be given
		Job job = Job.getInstance(configuration, "TASK_1");
		Task_1.configureJob(job,this.input,this.output);
		return job.waitForCompletion(true);
	}


	public static void configureJob(Job job, String[] pathIn, String pathOut) throws IOException {
		job.setJarByClass(Task_1.class);

		// Set the mapper class it must use
		job.setMapperClass(Task_1_Map.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(IntWritable.class);

		// Set the reducer class it must use
		job.setReducerClass(Task_1_Reduce.class);

		// The output will be Text
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);

		/**
		 * Specify here the parameters to send to the job
		 **/
		job.getConfiguration().setStrings("adIdColumn", "adId");
		job.getConfiguration().setStrings("siteIdColumn", "siteId");
		job.getConfiguration().setStrings("dateColumn", "publishedDate");


		job.getConfiguration().setStrings("filterDateStart", "2020-11-05");
		job.getConfiguration().setStrings("filterDateEnd", "2020-11-07");

		// The files the job will read from/write to
		FileInputFormat.addInputPath(job, new Path(pathIn[0]));
		FileOutputFormat.setOutputPath(job, new Path(pathOut));


	}
}

