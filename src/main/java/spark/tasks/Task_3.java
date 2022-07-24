package spark.tasks;

import org.apache.spark.sql.*;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.functions.col;

public class Task_3 {
    public static void run(SparkSession sparkSession, String[] pathIn, String pathOut) {

        // Read siteAds
        // Define the siteAds Schema
        StructType siteAdsSchema = new StructType()
                .add("adId", "long")
                .add("siteId", "long")
                .add("publishedDate", "timestamp")
                .add("impressions", "long");
        // Prepare the encoder to a dataset of SiteAds
        Dataset<Row> siteAdsDf = sparkSession.read().option("header", "true").option("mode", "DROPMALFORMED")
                .schema(siteAdsSchema).csv(pathIn[1]);

        // Read Sites
        // Define the Sites Schema
        StructType sitesSchema = new StructType()
                .add("siteId", "long")
                .add("siteName", "string");
        // Prepare the encoder to a dataset of Ads
        Dataset<Row> sitesDf = sparkSession.read().option("header", "true").option("mode", "DROPMALFORMED")
                .schema(sitesSchema).csv(pathIn[0]);

        // Join the siteAds with Ads
        Dataset<Row> joinedAds = siteAdsDf.alias("siteAds").join(functions.broadcast(sitesDf.alias("sites")),
                sitesDf.col("siteId").equalTo(siteAdsDf.col("siteId")),"right");

        joinedAds=joinedAds.groupBy("siteName").agg(
                functions.sum("impressions").as("sum_impressions"),
                functions.count("adId").as("count_adId"));

        joinedAds = joinedAds.withColumn("averageImpressions",
                functions.when(col("count_adId").isNull().or(col("count_adId").equalTo(0)),0)
                        .otherwise(
                                functions.round(
                                        new Column("sum_impressions").divide(new Column("count_adId"))
                                        , 3)
                        ));

        joinedAds = joinedAds.select("siteName","averageImpressions");
        joinedAds.coalesce(1).write().mode("overwrite").option("header", "true").csv(pathOut);
    }
}
