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
    // These constants are for storing the common values for the tuple that is generated by each mapper.
    public static final String TUPLE_SEPARATOR = "#";
    public static final String TABLE_ALIAS_ADS = "ads";
    public static final String TABLE_ALIAS_SITE_ADS = "siteAds";
    public static final int TUPLE_ALIAS_POS = 0;
    public static final int TUPLE_VALUE_POS = 1;

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

            // Count the number of commas in the line, if it is != 2, it is not a valid line
            long numberOfCommas = value.toString().chars().filter(ch -> ch == ',').count();
            if (numberOfCommas != 2) return;

            // Split the data
            String[] arrayValues = value.toString().split(",");

            // Get the values
            String adIdColName = context.getConfiguration().getStrings("adId")[0];
            String adPriceColName = context.getConfiguration().getStrings("adPriceColumn")[0];
            String adIdValue = Utils.getAttributeAds(arrayValues, adIdColName);
            String adPriceValue = Utils.getAttributeAds(arrayValues, adPriceColName);

            // Check if adIdValue or adPriceValue are null, skip the row
            if ((adIdValue == null)||(adPriceValue==null)) return;

            // Prepare the output tuple with the format ads#adPriceValue
            String finalOutput = createValueTuple(TABLE_ALIAS_ADS, adPriceValue);

            context.write(new Text(adIdValue), new Text(finalOutput));
        }
    }

    public static class Task_2_Map_SiteAds extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // The First row key has the columns, so we skip the first row
            if (key.get() == 0) return;

            // Count the number of commas in the line, if it is != 3, it is not a valid line
            long numberOfCommas = value.toString().chars().filter(ch -> ch == ',').count();
            if (numberOfCommas != 3) return;

            // Split the data
            String[] arrayValues = value.toString().split(",");

            // Get the column values
            String adIdColName = context.getConfiguration().getStrings("adId")[0];
            String impressionsColName = context.getConfiguration().getStrings("impressionsColumn")[0];
            String adIdValue = Utils.getAttributeSiteAds(arrayValues, adIdColName);
            String impressionsValue = Utils.getAttributeSiteAds(arrayValues, impressionsColName);

            // If there is no impressions, then the number of impressions is 0
            if (impressionsValue == null) impressionsValue = "0";

            // Prepare the output tuple with the format siteAds#impressionsValue
            String finalOutput = createValueTuple(TABLE_ALIAS_SITE_ADS, impressionsValue);

            context.write(new Text(adIdValue), new Text(finalOutput));
        }

    }

    public static class Task_2_Reduce extends Reducer<Text, Text, Text, Text> {
        /**
         * This function is used to filter the tuples that are generated by the mapper that generates the siteAds data.
         * It expects the format generated by {@link Task_2#createValueTuple}.
         *
         * @param tuple A tuple of data in the format of {@link Task_2#createValueTuple}.
         * @return A boolean value with true if the tuple is generated by the siteAds mapper.
         */
        public static boolean isSiteAdsTuple(Text tuple) {
            String[] tupleFields = tuple.toString().split(TUPLE_SEPARATOR);
            return tupleFields[TUPLE_ALIAS_POS].equals(TABLE_ALIAS_SITE_ADS);
        }

        /**
         * This function is used to get the Ad price of a given tuple generated by {@link Task_2#createValueTuple}.
         *
         * @param tuple A tuple of data in the format of {@link Task_2#createValueTuple}.
         * @return A double value of the ad price.
         */
        public static long getAdPrice(Text tuple) {
            String[] tupleFields = tuple.toString().split(TUPLE_SEPARATOR);
            return Long.parseLong(tupleFields[TUPLE_VALUE_POS]);
        }

        /**
         * This function is used to get the number of Impressions of a given tuple generated by {@link Task_2#createValueTuple}.
         *
         * @param tuple A tuple of data in the format of {@link Task_2#createValueTuple}.
         * @return A long value indicating the number of impressions for a given ad.
         */
        public static long getImpressionCount(Text tuple) {
            String[] tupleFields = tuple.toString().split(TUPLE_SEPARATOR);
            return Long.parseLong(tupleFields[TUPLE_VALUE_POS]);
        }


        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // Counter variables
            long adPrice = 0;
            long totalImpressions = 0;

            for (Text tuple : values) {
                // Check if the current value type
                if (isSiteAdsTuple(tuple)) {
                    // If it is a siteAds tuple, sum the number of impressions
                    totalImpressions += getImpressionCount(tuple);
                } else {
                    // If it is an Ads tuple, set the adPrice (only once)
                    adPrice = getAdPrice(tuple);
                }
            }

            // Finally calculate the revenue
            long revenue = adPrice * totalImpressions;
            // Format with one decimal
            String finalOutput = String.format("%d", revenue);
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

    /**
     * This function creates a string representation of a tuple of values. The tuple fields are separated by
     * the {@link Task_2#TUPLE_SEPARATOR} constant ({@link Task_2#TUPLE_SEPARATOR}) and each item position is determined by the constants
     * {@link Task_2#TABLE_ALIAS_ADS} and {@link Task_2#TABLE_ALIAS_SITE_ADS}.
     *
     * @param alias This param indicates the table alias or mapper origin of the tuple data, possible values are:
     * {@link Task_2#TABLE_ALIAS_ADS} and {@link Task_2#TABLE_ALIAS_SITE_ADS}.
     * @param value The value to be store into the tuple.
     * @return The tuple encoded as string with {@link Task_2#TUPLE_SEPARATOR} as separator.
     */
    public static String createValueTuple(String alias, String value) {
        ArrayList<String> outputTuple = new ArrayList<>();
        outputTuple.add(TUPLE_ALIAS_POS, alias);
        outputTuple.add(TUPLE_VALUE_POS, value);
        return String.join(TUPLE_SEPARATOR, outputTuple);
    }

}