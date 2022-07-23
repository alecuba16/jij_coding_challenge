package mapreduce.tasks;

import junit.framework.TestCase;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Task_2Test extends TestCase {
    private MapDriver<LongWritable, Text, Text, Text> mapAdsDriver;
    private MapDriver<LongWritable, Text, Text, Text> mapSiteAdsDriver;
    private ReduceDriver<Text, Text, Text, Text> reduceDriver;

    @Before
    public void setUp() {
        // Prepare ads mapper
        Task_2.Task_2_Map_Ads mapperAds = new Task_2.Task_2_Map_Ads();
        mapAdsDriver = new MapDriver<>();
        mapAdsDriver.setMapper(mapperAds);
        mapAdsDriver.getConfiguration().setStrings("keyColumn", "adId");
        mapAdsDriver.getConfiguration().setStrings("adPriceColumn", "adPrice");

        // Prepare siteAds mapper
        Task_2.Task_2_Map_SiteAds mapperSiteAds = new Task_2.Task_2_Map_SiteAds();
        mapSiteAdsDriver = new MapDriver<>();
        mapSiteAdsDriver.setMapper(mapperSiteAds);
        mapSiteAdsDriver.getConfiguration().setStrings("keyColumn", "adId");
        mapSiteAdsDriver.getConfiguration().setStrings("impressionsColumn", "impressions");

        // Prepare reducer
        Task_2.Task_2_Reduce reducer = new Task_2.Task_2_Reduce();
        reduceDriver = new ReduceDriver<>();
        reduceDriver.setReducer(reducer);
        reduceDriver.getConfiguration().setBoolean("writeHeader", true);
    }

    public void testMapAds() throws IOException {
        String siteId = "10";
        String price = "200";
        String line1 = String.format("%s,ad_999,%s", siteId, price);
        String finalOutput = Task_2.createValueTuple(Task_2.TABLE_ALIAS_ADS, price);
        mapAdsDriver.withInput(new LongWritable(1), new Text(line1))
                .withOutput(new Text(siteId), new Text(finalOutput))
                .runTest();
    }

    public void testMapSiteAds() throws IOException {
        String impressions = "24";
        String siteId = "10";
        String line1 = String.format("%s,11,2020-11-14T00:00:00+00:00,%s", siteId, impressions);
        String finalOutput = Task_2.createValueTuple(Task_2.TABLE_ALIAS_SITE_ADS, impressions);
        mapSiteAdsDriver.withInput(new LongWritable(1), new Text(line1))
                .withOutput(new Text(siteId), new Text(finalOutput))
                .runTest();
    }

    public void testReducer() throws IOException {
        String impressions = "24";
        String siteId = "10";
        String price = "200";
        String impressionTuple = Task_2.createValueTuple(Task_2.TABLE_ALIAS_SITE_ADS, impressions);
        String priceTuple = Task_2.createValueTuple(Task_2.TABLE_ALIAS_ADS, price);
        List<Text> values = new ArrayList<>();
        values.add(new Text(impressionTuple));
        values.add(new Text(priceTuple));

        String finalOutput = String.format("%.1f", Double.parseDouble(impressions) * Double.parseDouble(price));
        reduceDriver.withInput(new Text(siteId), values)
                .withOutput(new Text("adId"), new Text("revenue"))
                .withOutput(new Text(siteId), new Text(finalOutput))
                .runTest();
    }

    public void testCreateValueTuple() {
        String value = "200";
        String tuple = Task_2.createValueTuple(Task_2.TABLE_ALIAS_ADS, "200");
        String expectedValue = String.format("%s%s%s", Task_2.TABLE_ALIAS_ADS, Task_2.TUPLE_SEPARATOR, value);
        assertEquals(expectedValue, tuple);
    }

    public void testIsSiteAdsTuple() {
        Text adsTuple = new Text(String.format("%s%s%s", Task_2.TABLE_ALIAS_SITE_ADS, Task_2.TUPLE_SEPARATOR, ""));
        boolean isAdTuple = Task_2.Task_2_Reduce.isSiteAdsTuple(adsTuple);
        assertTrue(isAdTuple);
    }

    public void testGetAdPrice() {
        double expectedPrice = 200;
        String expectedPriceStr = Double.toString(expectedPrice);
        String priceTuple = Task_2.createValueTuple(Task_2.TABLE_ALIAS_ADS, expectedPriceStr);
        double returnedPrice = Task_2.Task_2_Reduce.getAdPrice(new Text(priceTuple));
        assertEquals(expectedPrice, returnedPrice);
    }

    public void testGetImpressions() {
        long expectedImpressions = 999;
        String expectedImpressionsStr = Long.toString(expectedImpressions);
        String impressionsTuple = Task_2.createValueTuple(Task_2.TABLE_ALIAS_SITE_ADS, expectedImpressionsStr);
        long returnedImpressions = Task_2.Task_2_Reduce.getImpressionCount(new Text(impressionsTuple));
        assertEquals(expectedImpressions, returnedImpressions);
    }

}