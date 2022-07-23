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

public class Task_3Test extends TestCase {
    private MapDriver<LongWritable, Text, Text, Text> mapSitesDriver;
    private MapDriver<LongWritable, Text, Text, Text> mapSiteAdsDriver;
    private ReduceDriver<Text, Text, Text, Text> reduceDriver;
    private ReduceDriver<Text, Text, Text, Text> combinerDriver;

    @Before
    public void setUp() {
        // Prepare ads mapper
        Task_3.Task_3_Map_Sites mapperSites = new Task_3.Task_3_Map_Sites();
        mapSitesDriver = new MapDriver<>();
        mapSitesDriver.setMapper(mapperSites);
        mapSitesDriver.getConfiguration().setStrings("siteIdColumn", "siteId");
        mapSitesDriver.getConfiguration().setStrings("siteNameColumn", "siteName");

        // Prepare siteAds mapper
        Task_3.Task_3_Map_SiteAds mapperSiteAds = new Task_3.Task_3_Map_SiteAds();
        mapSiteAdsDriver = new MapDriver<>();
        mapSiteAdsDriver.setMapper(mapperSiteAds);
        mapSiteAdsDriver.getConfiguration().setStrings("siteIdColumn", "siteId");
        mapSiteAdsDriver.getConfiguration().setStrings("impressionsColumn", "impressions");

        // Prepare combiner
        Task_3.Task_3_Combiner combiner = new Task_3.Task_3_Combiner();
        combinerDriver = new ReduceDriver<>();
        combinerDriver.setReducer(combiner);
        combinerDriver.getConfiguration().setBoolean("writeHeader", true);

        // Prepare reducer
        Task_3.Task_3_Reduce reducer = new Task_3.Task_3_Reduce();
        reduceDriver = new ReduceDriver<>();
        reduceDriver.setReducer(reducer);
        reduceDriver.getConfiguration().setBoolean("writeHeader", true);
    }

    public void testMapSitesAds() throws IOException {
        String adIdCount = "1";
        String impressions = "100";
        String siteId = "1";
        String siteName = "";
        String line1 = String.format("10,%s,2020-11-06T00:00:00+00:00,%s",siteId,impressions);
        String finalOutput = Task_3.createValueTuple(Task_3.TABLE_ALIAS_SITE_ADS, adIdCount,impressions,siteName);
        mapSiteAdsDriver.withInput(new LongWritable(1), new Text(line1))
                .withOutput(new Text(siteId), new Text(finalOutput))
                .runTest();
    }

    public void testMapSites() throws IOException {
        String adIdCount = "";
        String impressions = "";
        String siteId = "1";
        String siteName = "site_1";
        String line1 = String.format("%s,%s",siteId,siteName);
        String finalOutput = Task_3.createValueTuple(Task_3.TABLE_ALIAS_SITES, adIdCount,impressions,siteName);
        mapSitesDriver.withInput(new LongWritable(1), new Text(line1))
                .withOutput(new Text(siteId), new Text(finalOutput))
                .runTest();
    }

    public void testCombiner() throws IOException {
        String impressions = "23";
        String siteId = "10";
        String adIdCount = "1";
        String siteName = "site_1";
        String tupleSiteAds = Task_3.createValueTuple(Task_3.TABLE_ALIAS_SITE_ADS, adIdCount,impressions,"");

        List<Text> values = new ArrayList<>();
        values.add(new Text(tupleSiteAds));
        values.add(new Text(tupleSiteAds));
        values.add(new Text(tupleSiteAds));

        String finalOutput = String.format("%s%s%d%s%d%s%s",
                Task_3.TABLE_ALIAS_SITE_ADS,
                Task_3.TUPLE_SEPARATOR,
                (Long.parseLong(adIdCount)*3),
                Task_3.TUPLE_SEPARATOR,
                (Long.parseLong(impressions) * 3),
                Task_3.TUPLE_SEPARATOR,
                ""
                );

        String tupleSites = Task_3.createValueTuple(Task_3.TABLE_ALIAS_SITES, adIdCount,impressions,siteName);
        List<Text> values2 = new ArrayList<>();
        values2.add(new Text(tupleSites));
        String finalOutput2 = String.format("%s%s%s%s%s%s%s",
                Task_3.TABLE_ALIAS_SITES,
                Task_3.TUPLE_SEPARATOR,
                "0",
                Task_3.TUPLE_SEPARATOR,
                "0",
                Task_3.TUPLE_SEPARATOR,
                siteName
        );

        combinerDriver.withInput(new Text(siteId), values)
                .withOutput(new Text(siteId), new Text(finalOutput))
                .withInput(new Text(siteId), values2)
                .withOutput(new Text(siteId), new Text(finalOutput2))
                .runTest();
    }

    public void testReducer() throws IOException {
        String impressions = "24";
        String siteId = "10";
        String adIdCount = "1";
        String siteName = "site_1";
        String tupleSiteAds = Task_3.createValueTuple(Task_3.TABLE_ALIAS_SITE_ADS, adIdCount,impressions,siteName);
        String tupleSites = Task_3.createValueTuple(Task_3.TABLE_ALIAS_SITES, adIdCount,impressions,siteName);

        List<Text> values = new ArrayList<>();
        values.add(new Text(tupleSiteAds));
        values.add(new Text(tupleSiteAds));
        values.add(new Text(tupleSites));

        String finalOutput = String.format("%.1f", (Double.parseDouble(impressions) * 2) / (Long.parseLong(adIdCount)*2));
        reduceDriver.withInput(new Text(siteId), values)
                .withOutput(new Text("siteName"), new Text("averageImpressions"))
                .withOutput(new Text(siteName), new Text(finalOutput))
                .runTest();
    }


    public void testCreateValueTuple() {
        String impressions = "24";
        String adIdCount = "1";
        String siteName = "site_1";
        String tuple = Task_3.createValueTuple(Task_3.TABLE_ALIAS_SITE_ADS, adIdCount,impressions,siteName);

        String expectedValue = String.format("%s%s%s%s%s%s%s",
                Task_3.TABLE_ALIAS_SITE_ADS,
                Task_3.TUPLE_SEPARATOR,
                adIdCount,
                Task_3.TUPLE_SEPARATOR,
                impressions,
                Task_3.TUPLE_SEPARATOR,
                siteName
        );

        assertEquals(expectedValue, tuple);
    }

    public void testIsSiteAdsTuple() {
        Text siteAdsTuple = new Text(String.format("%s%s%s%s%s%s%s",
                Task_3.TABLE_ALIAS_SITE_ADS,
                Task_3.TUPLE_SEPARATOR,
                "",
                Task_3.TUPLE_SEPARATOR,
                "",
                Task_3.TUPLE_SEPARATOR,
                ""
        ));
        boolean isAdTuple = Task_3.isSiteAdsTuple(siteAdsTuple);
        assertTrue(isAdTuple);
    }

    public void testGetAdIdCount() {
        long expectedIdCount = 10;
        String expectedIdCountStr = Long.toString(expectedIdCount);

        String tuple = Task_3.createValueTuple(Task_3.TABLE_ALIAS_SITE_ADS, expectedIdCountStr,"","");
        long returnedIdCount = Task_3.getAdIdCount(new Text(tuple));
        assertEquals(expectedIdCount, returnedIdCount);
    }

    public void testGetImpressions() {
        long expectedImpressions = 10;
        String expectedImpressionsStr = Long.toString(expectedImpressions);
        String tuple = Task_3.createValueTuple(Task_3.TABLE_ALIAS_SITE_ADS, "",expectedImpressionsStr,"");
        long returnedImpressions = Task_3.getImpressionCount(new Text(tuple));
        assertEquals(expectedImpressions, returnedImpressions);
    }

    public void testGetSiteName() {
        String expectedSiteName = "testSite_1";
        String tuple = Task_3.createValueTuple(Task_3.TABLE_ALIAS_SITE_ADS, "","",expectedSiteName);
        String returnedSiteName = Task_3.getSiteName(new Text(tuple));
        assertEquals(expectedSiteName, returnedSiteName);
    }

    public void testGetTableAlias() {
        String expectedTableAlias = Task_3.TABLE_ALIAS_SITE_ADS;
        String tuple = Task_3.createValueTuple(expectedTableAlias, "","","");
        String returnedTableAlias = Task_3.getTableAlias(new Text(tuple));
        assertEquals(expectedTableAlias, returnedTableAlias);
    }

}