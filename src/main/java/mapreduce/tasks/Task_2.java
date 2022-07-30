package mapreduce.tasks;

import mapreduce.JobMapReduce;
import mapreduce.Utils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.*;

// Calculate revenue of the ads. Being revenue = adPrice * impressions
public class Task_2 extends JobMapReduce {

    /*
     * Two options for joining the information:
     * 1) 2 Jobs:
     *          first job maps ads, siteAds -> reduce on adId multiplying impressions * price
     *          second job generates the total revenue by mapping all values to a null key -> reduce summing all values
     * 2) Distribute the smaller table (ads) on the distributed cache, join on map operation, -> reduce on a nullkey
     *
     * The smaller table is relatively big >100MB, so option 2 may be memory bounded in small nodes, option 1 is possible
     * on reduced memory nodes. So option 1 is used in this case.
     */

    public static class Task_2_Map extends Mapper<LongWritable, Text, Text, IntWritable> {
        private static Map<String, Integer> adsPrices;

        public void fileToAdsPricesMap(Path filePath, String adIdColumnName) throws IOException {
            try (BufferedReader bufferedReader =
                         new BufferedReader(new FileReader(filePath.toString()))) {
                String line;
                while ((line = bufferedReader.readLine()) != null) {
                    if (line.contains(adIdColumnName)) continue;
                    String[] adsArray = line.split(",");
                    String adsId = adsArray[0];
                    Integer adsPrice = Integer.parseInt(adsArray[2]);
                    adsPrices.put(adsId, adsPrice);
                }
            }
        }

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            // Get the column names
            String adIdColumnName = context.getConfiguration().getStrings("adId")[0];
            adsPrices = new HashMap<>();
            URI[] adPricesFiles = context.getCacheFiles();
            if (adPricesFiles != null && adPricesFiles.length > 0) {
                for (URI adPricesFile : adPricesFiles) {
                    fileToAdsPricesMap(new Path(adPricesFile.getPath()), adIdColumnName);
                }
            }

        }

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // The First row key has the columns, so we skip the first row
            if (key.get() == 0) return;

            // Get the column names
            String adId = context.getConfiguration().getStrings("adId")[0];
            String impressions = context.getConfiguration().getStrings("impressionsColumn")[0];

            // Split the data
            String[] arrayValues = value.toString().split(",");

            // Check if array is completed
            if ((arrayValues.length < 4)) return;

            // Get the column names
            String adIdValue = Utils.getAttributeSiteAds(arrayValues, adId);
            String impressionsValue = Utils.getAttributeSiteAds(arrayValues, impressions);
            // If there is no impressions, then the number of impressions is 0
            if (impressionsValue == null) impressionsValue = "0";

            int revenue = 0;
            // Join operation
            if (adsPrices.containsKey(adIdValue)) {
                revenue = adsPrices.get(adIdValue) * Integer.parseInt(impressionsValue);
            }
            context.write(new Text(adIdValue), new IntWritable(revenue));
        }

    }

    public static class Task_2_Combiner extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            // Counter variables
            int revenue = 0;
            for (IntWritable tuple : values) {
                revenue += tuple.get();
            }
            context.write(key, new IntWritable(revenue));
        }
    }

    public static class Task_2_Reduce extends Reducer<Text, IntWritable, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            // Counter variables
            double revenue = 0;

            for (IntWritable tuple : values) {
                revenue += tuple.get();
            }

            // Format with one decimal
            String finalOutput = String.format("%.1f", revenue);
            // Output will be the adId,revenue with one decimal
            context.write(key, new Text(finalOutput));
        }

        // Override the setup method for adding the CSV column header
        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            // Get the value if write header is enabled
            boolean writeHeader = context.getConfiguration().getBoolean("writeHeader", true);
            if (writeHeader) {
                Text column = new Text("adId");
                Text values = new Text("revenue");
                context.write(column, values);
            }
        }
    }

    public Task_2() {
        this.input = null;
        this.output = null;
    }

    @Override
    public boolean run() throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = new Configuration();
        // Define the new job and the name it will be given
        Job job = Job.getInstance(configuration, "TASK_2");
        Task_2.configureJob(job, this.input, this.output);
        return job.waitForCompletion(true);
    }


    public static void configureJob(Job job, String[] pathIn, String pathOut) throws IOException {
        job.setJarByClass(Task_2.class);
        // Prepare the input paths and set to the mappers
        String pathAds = pathIn[0];
        Path pathSiteAds = new Path(pathIn[1]);
        FileInputFormat.setInputPaths(job, pathSiteAds);

        // add files to cache
        File folder = new File(pathAds);
        File[] listOfFiles = folder.listFiles();
        for (File file : listOfFiles) {
            if (file.isFile()) {
                job.addCacheFile(file.toURI());
            }
        }

        //Path inputPathDistributed = new Path(pathAds);
        //job.addCacheFile(inputPathDistributed.toUri());

        job.setCombinerClass(Task_2_Combiner.class);

        job.setMapperClass(Task_2.Task_2_Map.class);
        // Set the mapper output keys and values
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // Set the reducer class it must use
        job.setReducerClass(Task_2_Reduce.class);

        // The output will be Text
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        /**
         * Specify here the parameters to send to the job
         **/
        job.getConfiguration().setStrings("adId", "adId");
        job.getConfiguration().setStrings("adPriceColumn", "adPrice");
        job.getConfiguration().setStrings("impressionsColumn", "impressions");
        job.getConfiguration().set("mapred.textoutputformat.separator", ",");

        // Cleanup output path
        Path outputPath = new Path(pathOut);
        FileSystem fs = FileSystem.get(outputPath.toUri(), job.getConfiguration());
        fs.delete(outputPath, true);

        // The files the job will read from/write to
        FileOutputFormat.setOutputPath(job, new Path(pathOut));
    }
}