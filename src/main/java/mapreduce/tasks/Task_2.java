package mapreduce.tasks;

import mapreduce.JobMapReduce;
import mapreduce.Utils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class Task_2 extends JobMapReduce {

	public static class Task_2_Map_Ads extends Mapper<??, ??, ??, ??> {

		public void map(?? key, ?? value, Context context) throws IOException, InterruptedException {
//			######################
//			###	YOUR CODE HERE ###
//			######################

		}
	}

	public static class Task_2_Reduce extends Reducer<??, ??, ??, ??> {



		public void reduce(?? key, Iterable<??> values, Context context) throws IOException, InterruptedException {
//			######################
//			###	YOUR CODE HERE ###
//			######################

		}
	}

	public Task_2() {
		this.input = null;
		this.output = null;
	}

	public boolean run() throws IOException, ClassNotFoundException, InterruptedException {
		Configuration configuration = new Configuration();
		// Define the new job and the name it will be given
		Job job = Job.getInstance(configuration, "TASK_2");
		Task_2.configureJob(job,this.input,this.output);
        return job.waitForCompletion(true);
	}


	public static void configureJob(Job job, String[] pathIn, String pathOut) throws IOException, ClassNotFoundException, InterruptedException {
        job.setJarByClass(Task_2.class);


		String pathAds = pathIn[0];
		String pathSiteAds = pathIn[1];

		MultipleInputs.addInputPath(job, new Path(pathAds), TextInputFormat.class, Task_2_Map_Ads.class);
		MultipleInputs.addInputPath(job, new Path(pathSiteAds), TextInputFormat.class, null);

        job.setMapOutputKeyClass();
        job.setMapOutputValueClass();


        // Set the reducer class it must use
        job.setReducerClass(Task_2_Reduce.class);

        // The output will be Text
        job.setOutputKeyClass(??);
        job.setOutputValueClass(??);

        /**
	    * Specify here the parameters to send to the job
	    **/
		job.getConfiguration().setStrings("keyColumn", "");
		job.getConfiguration().setStrings("adPriceColumn", "");
		job.getConfiguration().setStrings("impressionsColumn", "");

        // The files the job will read from/write to
        FileOutputFormat.setOutputPath(job, new Path(pathOut));
       

    }
}

