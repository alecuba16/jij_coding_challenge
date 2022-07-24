package spark.common;

import org.apache.spark.sql.types.StructType;

import java.io.Serializable;

public class Ads implements Serializable {
    int adId;
    String adName;
    int adPrice;

    public int getAdId() {
        return adId;
    }

    public void setAdId(int adId) {
        this.adId = adId;
    }

    public String getAdName() {
        return adName;
    }

    public void setAdName(String adName) {
        this.adName = adName;
    }

    public int getAdPrice() {
        return adPrice;
    }

    public void setAdPrice(int adPrice) {
        this.adPrice = adPrice;
    }

    public Ads(int adId, String adName, int adPrice) {
        this.adId = adId;
        this.adName = adName;
        this.adPrice = adPrice;
    }

    public static StructType getSparkSchema(){
        return new StructType()
                .add("adId", "integer")
                .add("adName", "string")
                .add("adPrice", "int");
    }

    @Override
    public String toString() {
        return "Ads{" +
                "adId='" + adId + '\'' +
                ", adName='" + adName + '\'' +
                ", adPrice='" + adPrice + '\'' +
                '}';
    }
}