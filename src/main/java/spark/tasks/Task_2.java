package spark.tasks;

import org.apache.spark.sql.*;
import org.apache.spark.sql.types.StructType;
import static org.apache.spark.sql.functions.col;

public class Task_2 {
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

        // Read Ads
        // Define the Ads Schema
        StructType adsSchema = new StructType()
                .add("adId", "long")
                .add("adName", "string")
                .add("adPrice", "long");
        // Prepare the encoder to a dataset of Ads
        Dataset<Row> adsDf = sparkSession.read().option("header", "true").option("mode", "DROPMALFORMED")
                .schema(adsSchema).csv(pathIn[0]);
        // Select adId and adPrice
        adsDf = adsDf.select(col("adId"), col("adPrice"));

        // Aggregate the impressions by adId
        siteAdsDf = siteAdsDf.groupBy("adId").agg(functions.sum("impressions").alias("impressions"));

        // Join the siteAds with Ads
        Dataset<Row> joinedAds = siteAdsDf.alias("siteAds").join(functions.broadcast(adsDf.alias("ads")),
                adsDf.col("adId").equalTo(siteAdsDf.col("adId")),"right");

        joinedAds = joinedAds.select("ads.adId","impressions","adPrice");

        // Calculate the revenue
        Dataset<Row> revenueDf=joinedAds.withColumn("revenue",
                functions.when(col("impressions").isNull(),0)
                        .otherwise(new Column("adPrice").multiply(new Column("impressions"))));

        revenueDf=revenueDf.select("adId","revenue");
        revenueDf.coalesce(1).write().mode("overwrite").option("header", "true").csv(pathOut);
    }
}
