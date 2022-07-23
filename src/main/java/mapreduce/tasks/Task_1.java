package mapreduce.tasks;

import mapreduce.JobMapReduce;
import mapreduce.Utils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAccessor;

public class Task_1 extends JobMapReduce {
    public static class Task_1_Map extends Mapper<LongWritable, Text, IntWritable, IntWritable> {

        /**
         * The date format used to parse the date from the input file. Matches 2020-11-14T00:00:00+00:00 and 2020-11-14
         */
        private final DateTimeFormatter timePatterns = DateTimeFormatter.ofPattern("[yyyyMMdd][yyyy-MM-dd][yyyy-DDD]['T'[HHmmss][HHmm][HH:mm:ss][HH:mm][.SSSSSSSSS][.SSSSSS][.SSS][.SS][.S]][OOOO][O][z][XXXXX][XXXX]['['VV']']");

        /**
         * This function parses the date from string, it tries to match the most completed date-time.
         * It returns the date at the UTC time zone.
         *
         * @param dateStr, the string to be parsed.
         * @return The parsed date in UTC.
         */
        public ZonedDateTime parseDate(String dateStr) {
            // Try to get the most complete date from the optional format
            TemporalAccessor temporalAccessor = timePatterns.parseBest(dateStr, ZonedDateTime::from, LocalDateTime::from, LocalDate::from);
            // Convert to UTC depending on the class type of the best matching date.
            if (temporalAccessor instanceof ZonedDateTime) {
                return ((ZonedDateTime) temporalAccessor).withZoneSameInstant(ZoneOffset.UTC);
            } else if (temporalAccessor instanceof LocalDateTime) {
                return ((LocalDateTime) temporalAccessor).atZone(ZoneOffset.UTC);
            } else {
                return ((LocalDate) temporalAccessor).atStartOfDay(ZoneOffset.UTC);
            }
        }

        /**
         * This function checks that a given date {@code currentDate} is between the range determined
         * by {@code filterDateStart} and {@code filterDateEnd}, both included.
         *
         * @param currentDate     Date to be checked between ZonedDateTime and filterDateEnd.
         * @param filterDateStart Inital value of the range.
         * @param filterDateEnd   Final value of the range.
         * @return The parsed date in UTC.
         */
        public boolean isDateInRange(String currentDate, String filterDateStart, String filterDateEnd) {
            // Parse the date times
            ZonedDateTime currentDateDt = parseDate(currentDate);
            ZonedDateTime filterDateStartDt = parseDate(filterDateStart);
            ZonedDateTime filterDateEndDt = parseDate(filterDateEnd);
            return currentDateDt.compareTo(filterDateStartDt) >= 0 && currentDateDt.compareTo(filterDateEndDt) <= 0;
        }

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // The First row key has the columns, so we skip the first row
            if (key.get() == 0) return;

            // Get the column names
            String dateColumn = context.getConfiguration().getStrings("dateColumn")[0];
            String filterDateStart = context.getConfiguration().getStrings("filterDateStart")[0];
            String filterDateEnd = context.getConfiguration().getStrings("filterDateEnd")[0];

            // Split the data
            String[] arrayValues = value.toString().split(",");
            String dateColumnValue = Utils.getAttributeSiteAds(arrayValues, dateColumn);

            // Filter if the date is in the specified range and emit the value if it is inside the range
            if (isDateInRange(dateColumnValue, filterDateStart, filterDateEnd)) {
                context.write(new IntWritable(0), new IntWritable(1));
            }
        }
    }

    public static class Task_1_Reduce extends Reducer<IntWritable, IntWritable, Text, Text> {
        @Override
        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            // Initialize the count of the ads that have been published.
            long countOfPublishedAds = 0;
            for (IntWritable value : values) {
                countOfPublishedAds += value.get();
            }

            // Get the values of the range, to include the range into the output csv.
            String filterDateStart = context.getConfiguration().getStrings("filterDateStart")[0];
            String filterDateEnd = context.getConfiguration().getStrings("filterDateEnd")[0];

            // Format the final text output with the range
            String finalOutput = String.format("%s,%s", filterDateStart, filterDateEnd);

            context.write(new Text(Long.toString(countOfPublishedAds)), new Text(finalOutput));
        }

        // This overrride of the setup method is for including to the output file a header like a regular CSV
        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            // Get the value if write header is enabled
            boolean writeHeader = context.getConfiguration().getBoolean("writeHeader", true);
            if (writeHeader) {
                Text column = new Text("totalPublishedAds");
                Text values = new Text("dateIni,dateEnd");
                context.write(column, values);
            }
        }
    }

    public Task_1() {
        this.input = null;
        this.output = null;
    }

    @Override
    public boolean run() throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = new Configuration();
        // Define the new job and the name it will be given
        Job job = Job.getInstance(configuration, "TASK_1");
        Task_1.configureJob(job, this.input, this.output);
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
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        /**
         * Specify here the parameters to send to the job
         **/
        job.getConfiguration().setStrings("adIdColumn", "adId");
        job.getConfiguration().setStrings("siteIdColumn", "siteId");
        job.getConfiguration().setStrings("dateColumn", "publishedDate");
        job.getConfiguration().setStrings("filterDateStart", "2020-11-05");
        job.getConfiguration().setStrings("filterDateEnd", "2020-11-07");

        // This is for having a csv like output, instead space-separated key-values
        job.getConfiguration().set("mapred.textoutputformat.separator", ",");

        // Cleanup output path
        Path outputPath = new Path(pathOut);
        FileSystem fs = FileSystem.get(outputPath.toUri(), job.getConfiguration());
        fs.delete(outputPath, true);

        // The files the job will read from/write to
        FileInputFormat.addInputPath(job, new Path(pathIn[0]));
        FileOutputFormat.setOutputPath(job, new Path(pathOut));
    }
}

