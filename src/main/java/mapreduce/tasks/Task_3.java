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

public class Task_3 extends JobMapReduce {

	public static class Task_3_Map extends Mapper<??, ??, ??, ??> {

	//			######################
	//			###	YOUR CODE HERE ###
	//			######################
	}



	public static class Task_3_Reduce extends Reducer<??, ???, ??, ??> {

		//			######################
		//			###	YOUR CODE HERE ###
		//			######################


	}

	public Task_3() {
		this.input = null;
		this.output = null;
	}

	public boolean run() throws IOException, ClassNotFoundException, InterruptedException {
		Configuration configuration = new Configuration();
		// Define the new job and the name it will be given
		Job job = Job.getInstance(configuration, "TASK_3");
		Task_3.configureJob(job,this.input,this.output);
        return job.waitForCompletion(true);
	}


	public static void configureJob(Job job, String[] pathIn, String pathOut)  {
        job.setJarByClass(Task_3.class);

		String path = pathIn[0];

		MultipleInputs.addInputPath(job, new Path(path), TextInputFormat.class, Task_3_Map.class);

        job.setMapOutputKeyClass();
        job.setMapOutputValueClass();

		// Set a combiner
		job.setCombinerClass(?);

        // Set the reducer class it must use
        job.setReducerClass();


        // The output will be Text
        job.setOutputKeyClass();
        job.setOutputValueClass();

        /**
	    * Specify here the parameters to send to the job
	    **/
		job.getConfiguration().setStrings("keyColumn", "");


        // The files the job will read from/write to
        FileOutputFormat.setOutputPath(job, new Path(pathOut));
       

    }
}

