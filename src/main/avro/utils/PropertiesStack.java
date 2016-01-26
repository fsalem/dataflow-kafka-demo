package utils;

public class PropertiesStack {
	private static final String KAFKA_SERVER_URL = "86.50.170.143";
	private static final String ZOOKEEPER_SERVER_URL = "104.155.91.159";

	/*
	 * private static final String FILE_NAME="params.properties"; private static
	 * final Properties properties; static{ properties = new Properties(); try {
	 * properties.load(new FileReader(new File(FILE_NAME))); } catch
	 * (FileNotFoundException e) { e.printStackTrace(); } catch (IOException e)
	 * { e.printStackTrace(); } }
	 */
	private PropertiesStack() {
	}

	public static String getKafkaBootstrapServers() {
		// return properties.getProperty("bootstrap.servers");
		return KAFKA_SERVER_URL + ":9092";
	}

	public static String getZookeeperConnect() {
		// return properties.getProperty("zookeeper.connect");
		return ZOOKEEPER_SERVER_URL + ":2181";
	}

	public static String getKafkaGroupId() {
		// return properties.getProperty("group.id");
		return "test";
	}
	
	public static String getKafkaTopic() {
		// return properties.getProperty("group.id");
		return "tweets2";
	}
	
	public static String getKafkaTopicResult() {
		// return properties.getProperty("group.id");
		return "tweets2-result";
	}

	public static String getTwitterAPIKey() {
		// return properties.getProperty("twitter.api.key");
		return "lITUqHABkTAMc8aWAIh2hD82h";
	}

	public static String getTwitterAPISecret() {
		// return properties.getProperty("twitter.api.secret");
		return "2tJ7NmiTXBApbefXEr6QM4CHJfjM2U4GQBbEJvwyix8CUSUUuQ";
	}

	public static String getTwitterAccessToken() {
		// return properties.getProperty("twitter.access.token");
		return "811647516-UaHY9C0cGqqxTCMzo4BXGwgSqTq9LqtsIOXV7uLW";
	}

	public static String getTwitterAccessTokenSecret() {
		// return properties.getProperty("twitter.access.token.secret");
		return "q2P0Suz1LUdERqzhOYm6zhD63ZpYwbyB0a7dY8SAPXncA";
	}

}
