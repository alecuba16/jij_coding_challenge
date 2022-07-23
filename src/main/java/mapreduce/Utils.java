package mapreduce;



public class Utils {

	private static final String[] attributesAds = new String[] {
		"adId"
		,"adName"
		,"adPrice"
	};
	
	public static String getAttributeAds(String[] row, String attribute) {
		for (int i = 0; i < attributesAds.length; i++) {
			if (attributesAds[i].equals(attribute)) {
				return row[i];
			}
		}
		return null;
	}

	private static final String[] attributesSiteAds = new String[] {
			"adId"
			,"siteId"
			,"publishedDate"
			,"impressions"
	};

	public static String getAttributeSiteAds(String[] row, String attribute) {
		for (int i = 0; i < attributesSiteAds.length; i++) {
			if (attributesSiteAds[i].equals(attribute)) {
				return row[i];
			}
		}
		return null;
	}

	private static final String[] attributesSites = new String[] {
			"siteId"
			,"siteName"
	};

	public static String getAttributesSites(String[] row, String attribute) {
		for (int i = 0; i < attributesSites.length; i++) {
			if (attributesSites[i].equals(attribute)) {
				return row[i];
			}
		}
		return null;
	}


}
