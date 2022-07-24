package spark.common;

import org.apache.spark.sql.types.StructType;

import java.io.Serializable;
import java.sql.Timestamp;

public class SiteAds implements Serializable {
    int adId;
    int siteId;
    Timestamp publishedDate;
    int impressions;

    public int getAdId() {
        return adId;
    }

    public void setAdId(int adId) {
        this.adId = adId;
    }

    public int getSiteId() {
        return siteId;
    }

    public void setSiteId(int siteId) {
        this.siteId = siteId;
    }

    public Timestamp getPublishedDate() {
        return publishedDate;
    }

    public void setPublishedDate(Timestamp publishedDate) {
        this.publishedDate = publishedDate;
    }

    public int getImpressions() {
        return impressions;
    }

    public void setImpressions(int impressions) {
        this.impressions = impressions;
    }

    public SiteAds(int adId, int siteId, Timestamp publishedDate, int impressions) {
        this.adId = adId;
        this.siteId = siteId;
        this.publishedDate = publishedDate;
        this.impressions = impressions;
    }

    public static StructType getSparkSchema(){
        return new StructType()
                .add("adId", "integer")
                .add("siteId", "integer")
                .add("publishedDate", "timestamp")
                .add("impressions", "integer");
    }

    @Override
    public String toString() {
        return "SiteAds{" +
                "adId='" + adId + '\'' +
                ", siteId='" + siteId + '\'' +
                ", publishedDate=" + publishedDate +
                ", impressions='" + impressions + '\'' +
                '}';
    }
}