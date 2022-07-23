package mapreduce.tasks;

import junit.framework.TestCase;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import java.io.IOException;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

public class Task_1Test extends TestCase {
    private MapDriver<LongWritable, Text, IntWritable, IntWritable> mapDriver;
    private ReduceDriver<IntWritable, IntWritable, Text, Text> reduceDriver;

    @Before
    public void setUp() {
        Task_1.Task_1_Map mapper = new Task_1.Task_1_Map();
        Task_1.Task_1_Reduce reducer = new Task_1.Task_1_Reduce();
        mapDriver = new MapDriver<>();
        mapDriver.setMapper(mapper);
        reduceDriver = new ReduceDriver<>();
        reduceDriver.setReducer(reducer);

        mapDriver.getConfiguration().setStrings("dateColumn", "publishedDate");
        mapDriver.getConfiguration().setStrings("filterDateStart", "2020-11-05");
        mapDriver.getConfiguration().setStrings("filterDateEnd", "2020-11-07");

        reduceDriver.getConfiguration().setStrings("filterDateStart", "2020-11-05");
        reduceDriver.getConfiguration().setStrings("filterDateEnd", "2020-11-07");
        reduceDriver.getConfiguration().setBoolean("writeHeader", true);
    }

    public void testMap() throws IOException {
        String line1 = "1,0,2020-11-06T00:00:00+00:00,24";
        mapDriver.withInput(new LongWritable(1), new Text(line1))
                .withOutput(new IntWritable(0), new IntWritable(1))
                .runTest();
    }

    public void testReducer() throws IOException {
        List<IntWritable> values = new ArrayList<>();
        values.add(new IntWritable(1));
        values.add(new IntWritable(1));

        String filterDateStart = reduceDriver.getConfiguration().getStrings("filterDateStart")[0];
        String filterDateEnd = reduceDriver.getConfiguration().getStrings("filterDateEnd")[0];
        String finalOutput = String.format("%s,%s", filterDateStart, filterDateEnd);

        reduceDriver.withInput(new IntWritable(1), values)
                .withOutput(new Text("totalPublishedAds"), new Text("dateIni,dateEnd"))
                .withOutput(new Text("2"), new Text(finalOutput))
                .runTest();
    }


    public void testIsDateInRange() {
        Task_1.Task_1_Map Task_1 = new Task_1.Task_1_Map();
        String filterDateStart = "2022-01-01";
        String filterDateEnd = "2022-01-31";
        assertTrue(Task_1.isDateInRange("2022-01-15T00:00:00+00:00", filterDateStart, filterDateEnd));
        assertTrue(Task_1.isDateInRange("2022-01-01T00:00:00+00:00", filterDateStart, filterDateEnd));
        assertTrue(Task_1.isDateInRange("2022-01-31T00:00:00+00:00", filterDateStart, filterDateEnd));
        assertFalse(Task_1.isDateInRange("2022-02-02T00:00:00+00:00", filterDateStart, filterDateEnd));
        assertFalse(Task_1.isDateInRange("2021-12-31T00:00:00+00:00", filterDateStart, filterDateEnd));

    }

    public void testParseDate() {
        Task_1.Task_1_Map Task_1 = new Task_1.Task_1_Map();
        ZonedDateTime dateToTest = ZonedDateTime.now().withZoneSameInstant(ZoneOffset.UTC);
        String dateToTestStr = dateToTest.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME);
        assertEquals(Task_1.parseDate(dateToTestStr), dateToTest);
    }
}