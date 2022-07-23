package mapreduce.tasks;

import mapreduce.JobMapReduce;
import mapreduce.Utils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;

// Calculate revenue of the ads. Being revenue = adPrice * impressions
public class Task_2 extends JobMapReduce {
    public static final String TUPLE_SEPARATOR = "#";
    public static final String TABLE_ALIAS_ADS = "ads";
    public static final String TABLE_ALIAS_SITE_ADS = "siteAds";
    public static final int TUPLE_ALIAS_POS = 0;
    public static final int TUPLE_VALUE_POS = 1;

    public static String createValueTuple(String alias, String value) {
        ArrayList<String> outputTuple = new ArrayList<>();
        outputTuple.add(TUPLE_ALIAS_POS, alias);
        outputTuple.add(TUPLE_VALUE_POS, value);
        return String.join(TUPLE_SEPARATOR, outputTuple);
    }

    public static boolean isSiteAdsTuple(Text tuple) {
        String[] tupleFields = tuple.toString().split(TUPLE_SEPARATOR);
        return tupleFields[TUPLE_ALIAS_POS].equals(TABLE_ALIAS_SITE_ADS);
    }

    public static double getAdPrice(Text tuple) {
        String[] tupleFields = tuple.toString().split(TUPLE_SEPARATOR);
        return Double.parseDouble(tupleFields[TUPLE_VALUE_POS]);
    }

    public static long getImpressions(Text tuple) {
        String[] tupleFields = tuple.toString().split(TUPLE_SEPARATOR);
        return Long.parseLong(tupleFields[TUPLE_VALUE_POS]);
    }

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
    public static class Task_2_Map_Ads extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // First key has the columns, we have to find the positions of the columns we want to use
            if (key.get() == 0) return;

            // Get the column names
            String keyColumn = context.getConfiguration().getStrings("keyColumn")[0];
            String adPrice = context.getConfiguration().getStrings("adPriceColumn")[0];

            // Split the data
            String[] arrayValues = value.toString().split(",");

            // Get the values
            String keyColumnValue = Utils.getAttributeAds(arrayValues, keyColumn);
            String adPriceValue = Utils.getAttributeAds(arrayValues, adPrice);

            // Prepare the output tuple with the format ads#adPriceValue
            String finalOutput = createValueTuple(TABLE_ALIAS_ADS, adPriceValue);

            context.write(new Text(keyColumnValue), new Text(finalOutput));
        }
    }

    public static class Task_2_Map_SiteAds extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // First key has the columns, we have to find the positions of the columns we want to use
            if (key.get() == 0) return;

            // Get the column names
            String keyColumn = context.getConfiguration().getStrings("keyColumn")[0];
            String impressions = context.getConfiguration().getStrings("impressionsColumn")[0];

            // Split the data
            String[] arrayValues = value.toString().split(",");

            // Get the values
            String keyColumnValue = Utils.getAttributeSiteAds(arrayValues, keyColumn);
            String impressionsValue = Utils.getAttributeSiteAds(arrayValues, impressions);

            // Prepare the output tuple with the format siteAds#impressionsValue
            String finalOutput = createValueTuple(TABLE_ALIAS_SITE_ADS, impressionsValue);

            context.write(new Text(keyColumnValue), new Text(finalOutput));
        }

    }

    public static class Task_2_Reduce extends Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            double adPrice = 0;
            long totalImpressions = 0;
            for (Text tuple : values) {
                if (isSiteAdsTuple(tuple)) {
                    totalImpressions += getImpressions(tuple);
                } else {
                    adPrice = getAdPrice(tuple);
                }
            }
            double revenue = adPrice * totalImpressions;

            String finalOutput = String.format("%.1f", revenue);
            // Output will be the adId,revenue
            context.write(key, new Text(finalOutput));
        }

        // Override the setup method for adding the CSV column header
        @Override
        public void setup(Context context) throws IOException, InterruptedException {
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

        String pathAds = pathIn[0];
        String pathSiteAds = pathIn[1];

        MultipleInputs.addInputPath(job, new Path(pathAds), TextInputFormat.class, Task_2_Map_Ads.class);
        MultipleInputs.addInputPath(job, new Path(pathSiteAds), TextInputFormat.class, Task_2_Map_SiteAds.class);

        // Set the mapper output keys and values
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        // Set the reducer class it must use
        job.setReducerClass(Task_2_Reduce.class);

        // The output will be Text
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        /**
         * Specify here the parameters to send to the job
         **/
        job.getConfiguration().setStrings("keyColumn", "adId");
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