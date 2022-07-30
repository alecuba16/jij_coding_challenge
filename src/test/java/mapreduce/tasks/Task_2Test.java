package mapreduce.tasks;

import junit.framework.TestCase;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;

import java.io.File;
import java.io.IOException;
import org.apache.hadoop.fs.Path;
import java.util.ArrayList;
import java.util.List;


public class Task_2Test extends TestCase {
    private MapDriver<LongWritable, Text, Text, IntWritable> mapSiteAdsDriver;
    private ReduceDriver<Text, IntWritable, Text, Text> reduceDriver;
    private Task_2.Task_2_Map mapperSiteAds;

    @Before
    public void setUp() {
        // Prepare siteAds mapper
        mapperSiteAds = new Task_2.Task_2_Map();
        mapSiteAdsDriver = new MapDriver<>();
        mapSiteAdsDriver.setMapper(mapperSiteAds);
        mapSiteAdsDriver.getConfiguration().setStrings("adId", "adId");
        mapSiteAdsDriver.getConfiguration().setStrings("impressionsColumn", "impressions");

        Path adscsv = new Path("src/test/resources/ads.csv");
        mapSiteAdsDriver.addCacheFile(adscsv.toUri());

        // Prepare reducer
        Task_2.Task_2_Reduce reducer = new Task_2.Task_2_Reduce();
        reduceDriver = new ReduceDriver<>();
        reduceDriver.setReducer(reducer);
        reduceDriver.getConfiguration().setBoolean("writeHeader", true);
    }

    public void testMapSiteAds() throws IOException {
        Integer impressions = 24;
        String siteId = "1";
        Integer adPrice = 7;

        String line1 = String.format("%s,11,2020-11-14T00:00:00+00:00,%d", siteId, impressions);
        IntWritable finalOutput = new IntWritable(adPrice * impressions);
        mapSiteAdsDriver.withInput(new LongWritable(1), new Text(line1))
                .withOutput(new Text(siteId), finalOutput)
                .runTest();
    }

    public void testReducer() throws IOException {
        String impressions = "24";
        String siteId = "1";
        String price = "7";

        int revenue = Integer.parseInt(price) * Integer.parseInt(impressions);
        List<IntWritable> values = new ArrayList<>();
        values.add(new IntWritable(revenue));

        String finalOutput = String.format("%.1f", Double.parseDouble(impressions) * Double.parseDouble(price));
        reduceDriver.withInput(new Text(siteId), values)
                .withOutput(new Text("adId"), new Text("revenue"))
                .withOutput(new Text(siteId), new Text(finalOutput))
                .runTest();
    }
}