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

import java.io.*;
import java.util.*;

public class Task_3 extends JobMapReduce {
    public static final String TUPLE_SEPARATOR = "#";
    public static final String TABLE_ALIAS_SITES = "sites";
    public static final String TABLE_ALIAS_SITE_ADS = "siteAds";
    private static final int TUPLE_ALIAS_POS = 0;
    private static final int TUPLE_ADID_COUNT_POS = 1;
    private static final int TUPLE_IMPRESSIONS_POS = 2;
    private static final int TUPLE_SITENAME_POS = 3;

    public static String createValueTuple(String alias, String adIdCount, String impressions, String siteName) {
        ArrayList<String> outputTuple = new ArrayList<>();
        outputTuple.add(TUPLE_ALIAS_POS, alias);
        outputTuple.add(TUPLE_ADID_COUNT_POS, adIdCount);
        outputTuple.add(TUPLE_IMPRESSIONS_POS, impressions);
        outputTuple.add(TUPLE_SITENAME_POS, siteName);
        return String.join(TUPLE_SEPARATOR, outputTuple);
    }

    public static boolean isSiteAdsTuple(Text tuple) {
        String[] tupleFields = tuple.toString().split(TUPLE_SEPARATOR);
        return tupleFields[TUPLE_ALIAS_POS].equals(TABLE_ALIAS_SITE_ADS);
    }

    public static long getAdIdCount(Text tuple) {
        String[] tupleFields = tuple.toString().split(TUPLE_SEPARATOR);
        return Long.parseLong(tupleFields[TUPLE_ADID_COUNT_POS]);
    }

    public static long getImpressionCount(Text tuple) {
        String[] tupleFields = tuple.toString().split(TUPLE_SEPARATOR);
        return Long.parseLong(tupleFields[TUPLE_IMPRESSIONS_POS]);
    }

    public static String getSiteName(Text tuple) {
        String[] tupleFields = tuple.toString().split(TUPLE_SEPARATOR);
        return tupleFields[TUPLE_SITENAME_POS];
    }

    public static String getTableAlias(Text tuple) {
        String[] tupleFields = tuple.toString().split(TUPLE_SEPARATOR);
        return tupleFields[TUPLE_ALIAS_POS];
    }

    public static class Task_3_Map_SiteAds extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // First key has the columns, we have to find the positions of the columns we want to use
            if (key.get() == 0) return;

            // Get the column names
            String siteIdColumn = context.getConfiguration().getStrings("siteIdColumn")[0];
            String impressionsColumn = context.getConfiguration().getStrings("impressionsColumn")[0];

            // Split the data
            String[] arrayValues = value.toString().split(",");

            // Get the values
            String siteIdColumnValue = Utils.getAttributeSiteAds(arrayValues, siteIdColumn);
            String impressionsColumnValue = Utils.getAttributeSiteAds(arrayValues, impressionsColumn);

            String finalOutput = createValueTuple(TABLE_ALIAS_SITE_ADS, "1", impressionsColumnValue, "");

            context.write(new Text(siteIdColumnValue), new Text(finalOutput));
        }
    }

    public static class Task_3_Combiner extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            long adidCount = 0;
            long impressionsSum = 0;
            String tableAlias = "";
            String siteName = "";
            for (Text tuple : values) {
                tableAlias = getTableAlias(tuple);
                if (isSiteAdsTuple(tuple)) {
                    adidCount += getAdIdCount(tuple);
                    impressionsSum += getImpressionCount(tuple);
                } else {
                    siteName = getSiteName(tuple);
                }
            }
            // Prepare the output tuple
            String finalOutput = createValueTuple(tableAlias, Long.toString(adidCount), Long.toString(impressionsSum), siteName);
            context.write(new Text(key), new Text(finalOutput));
        }
    }

    public static class Task_3_Map_Sites extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // First key has the columns, we have to find the positions of the columns we want to use
            if (key.get() == 0) return;

            // Get the column names
            String siteIdColumn = context.getConfiguration().getStrings("siteIdColumn")[0];
            String siteNameColumn = context.getConfiguration().getStrings("siteNameColumn")[0];

            // Split the data
            String[] arrayValues = value.toString().split(",");

            // Get the values
            String siteIdColumnValue = Utils.getAttributesSites(arrayValues, siteIdColumn);
            String siteNameColumnValue = Utils.getAttributesSites(arrayValues, siteNameColumn);

            String finalOutput = createValueTuple(TABLE_ALIAS_SITES, "", "", siteNameColumnValue);

            context.write(new Text(siteIdColumnValue), new Text(finalOutput));
        }
    }

    public static class Task_3_Reduce extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            long adidCount = 0;
            long impressionsSum = 0;
            String siteName = "";

            for (Text tuple : values) {
                if (isSiteAdsTuple(tuple)) {
                    adidCount += getAdIdCount(tuple);
                    impressionsSum += getImpressionCount(tuple);
                } else {
                    siteName = getSiteName(tuple);
                }
            }

            Double avgImpressionsBySite = adidCount != 0 ? (double) impressionsSum / adidCount : null;
            String finalOutput = String.format("%.1f", avgImpressionsBySite);

            context.write(new Text(siteName), new Text(finalOutput));
        }

        // Override the run method for adding the CSV column header
        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            boolean writeHeader = context.getConfiguration().getBoolean("writeHeader", true);
            if (writeHeader) {
                Text column = new Text("siteName");
                Text values = new Text("averageImpressions");
                context.write(column, values);
            }
        }
    }

    public Task_3() {
        this.input = null;
        this.output = null;
    }

    @Override
    public boolean run() throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = new Configuration();
        // Define the new job and the name it will be given
        Job job = Job.getInstance(configuration, "TASK_3");
        Task_3.configureJob(job, this.input, this.output);
        return job.waitForCompletion(true);
    }


    public static void configureJob(Job job, String[] pathIn, String pathOut) throws IOException {
        job.setJarByClass(Task_3.class);

        String sitesPath = pathIn[0];
        String sitesAdsPath = pathIn[1];
        // Configurations
        job.getConfiguration().set("mapred.textoutputformat.separator", ",");
        job.getConfiguration().setStrings("siteIdColumn", "siteId");
        job.getConfiguration().setStrings("impressionsColumn", "impressions");
        job.getConfiguration().setStrings("siteNameColumn", "siteName");

        // Input mappers
        MultipleInputs.addInputPath(job, new Path(sitesPath), TextInputFormat.class, Task_3.Task_3_Map_Sites.class);
        MultipleInputs.addInputPath(job, new Path(sitesAdsPath), TextInputFormat.class, Task_3.Task_3_Map_SiteAds.class);

        // The mappers output classes
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        // Set a combiner
        job.setCombinerClass(Task_3_Combiner.class);

        // Set the reducer class it must use
        job.setReducerClass(Task_3.Task_3_Reduce.class);

        // The output will be Text
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Cleanup output path
        Path outputPath = new Path(pathOut);
        FileSystem fs = FileSystem.get(outputPath.toUri(), job.getConfiguration());
        fs.delete(outputPath, true);

        // The files the job will read from/write to
        FileOutputFormat.setOutputPath(job, new Path(pathOut));


    }
}
