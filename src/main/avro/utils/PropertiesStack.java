package utils;


import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

public class PropertiesStack {
	private static final String FILE_NAME="params.properties";
	private static final Properties properties;
	static{
		properties = new Properties();
		try {
			properties.load(new FileReader(new File(FILE_NAME)));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	private PropertiesStack() {
	}
	
	public static String getKafkaBootstrapServers(){
		return properties.getProperty("bootstrap.servers");
	}
	
	public static String getZookeeperConnect(){
		return properties.getProperty("zookeeper.connect");
	}
	
	public static String getKafkaGroupId(){
		return properties.getProperty("group.id");
	}
	
	public static String getTwitterAPIKey(){
		return properties.getProperty("twitter.api.key");
	}
	
	public static String getTwitterAPISecret(){
		return properties.getProperty("twitter.api.secret");
	}
	
	public static String getTwitterAccessToken(){
		return properties.getProperty("twitter.access.token");
	}
	
	public static String getTwitterAccessTokenSecret(){
		return properties.getProperty("twitter.access.token.secret");
	}
	
	
	
	
}
