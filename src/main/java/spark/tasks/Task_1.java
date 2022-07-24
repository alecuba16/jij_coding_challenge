package spark.tasks;

import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.StructType;
import spark.common.SiteAds;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.sql.Timestamp;
import java.util.Date;
import java.util.TimeZone;

public class Task_1 {
    public static boolean isDateInRange(Timestamp currentDate, Timestamp filterDateStart, Timestamp filterDateEnd) {
        return currentDate.compareTo(filterDateStart) >= 0 && currentDate.compareTo(filterDateEnd) <= 0;
    }

    public static void run(SparkSession sparkSession, String[] pathIn, String pathOut) throws ParseException {
        String inputPath = pathIn[0];

        // Read siteAds
        // Get spark schema
        StructType siteAdsSchema = SiteAds.getSparkSchema();
        // Prepare the encoder to a dataset of SiteAds
        Encoder<SiteAds> siteAdsEncoder = Encoders.bean(SiteAds.class);
        Dataset<SiteAds> siteAdsDf = sparkSession.read().option("header", "true").schema(siteAdsSchema).csv(inputPath).as(siteAdsEncoder);

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
        Dataset<SiteAds> filteredSiteAds = siteAdsDf.filter((FilterFunction<SiteAds>)
                siteAd -> isDateInRange(siteAd.getPublishedDate(), filterDateIni, filterDateEnd));
        Dataset<Row> countedSiteAds = filteredSiteAds.agg(functions.count("adId").alias("totalPublishedAds"),
                functions.lit(filterDateIniStr).alias("dateIni"),
                functions.lit(filterDateEndStr).alias("dateEnd"));
        countedSiteAds.write().mode("overwrite").option("header", "true").csv(pathOut);
    }
}
