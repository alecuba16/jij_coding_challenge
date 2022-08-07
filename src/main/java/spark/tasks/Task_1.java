package spark.tasks;

import org.apache.spark.sql.*;
import org.apache.spark.sql.types.StructType;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.sql.Timestamp;
import java.util.Date;
import java.util.TimeZone;

public class Task_1 {

    public static void run(SparkSession sparkSession, String[] pathIn, String pathOut) throws ParseException {
        String inputPath = pathIn[0];

        // Read siteAds
        // Define the siteAds Schema
        StructType siteAdsSchema = new StructType()
                .add("adId", "long")
                .add("siteId", "long")
                .add("publishedDate", "timestamp")
                .add("impressions", "long");
        // Prepare the encoder to a dataset of SiteAds
        Dataset<Row> siteAdsDf = sparkSession.read().option("header", "true").option("mode", "DROPMALFORMED")
                .schema(siteAdsSchema).csv(inputPath);

        String filterDateIniStr = "2020-11-05";
        String filterDateEndStr = "2020-11-07";

        // Set the formatter format and the default timezone of the formatter to UTC
        DateFormat dateFormatter = new SimpleDateFormat("yyyy-MM-dd");
        dateFormatter.setTimeZone(TimeZone.getTimeZone("UTC"));
        // Convert dates to timestamp
        Date filterDateIniD = dateFormatter.parse(filterDateIniStr);
        Date filterDateEndD = dateFormatter.parse(filterDateEndStr);
        Timestamp filterDateIni = new Timestamp(filterDateIniD.getTime());
        Timestamp filterDateEnd = new Timestamp(filterDateEndD.getTime());

        // Define the date filter
        Dataset<Row> filteredSiteAds = siteAdsDf.filter(new Column("publishedDate").between(filterDateIni, filterDateEnd));
        Dataset<Row> countedSiteAds = filteredSiteAds.agg(functions.count("adId").alias("totalPublishedAds"),
                functions.lit(filterDateIniStr).alias("dateIni"),
                functions.lit(filterDateEndStr).alias("dateEnd"));
        countedSiteAds.write().mode("overwrite").option("header", "true").csv(pathOut);
    }
}
