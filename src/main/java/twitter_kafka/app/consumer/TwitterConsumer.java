package twitter_kafka.app.consumer;
import com.mongodb.DBObject;

import com.google.gson.JsonParser;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.DBCollection;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.util.JSON;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Properties;

public class TwitterConsumer {
    //    public static void main(String[] args) throws IOException {
//        new KafkaConsumerMongoDb().run();
//    }
    public TwitterConsumer() { run();}
    public static ArrayList<String> TweetList = new ArrayList<String>();
    public static KafkaConsumer<String, String> createConsumer(String topic){

        String bootstrapServers = "localhost:9092";
        String groupId = "my-first-app3";
        // create consumer configs
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        properties.setProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        properties.setProperty(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
        consumer.subscribe(Arrays.asList(topic));

        return consumer;

    }
    
	/*
	 * private static MongoCollection<Document> createLink() {
	 * 
	 * MongoClientURI uri = new MongoClientURI("https://localhost:9200");
	 * MongoClient mongoClient = new MongoClient(uri); MongoDatabase database =
	 * mongoClient.getDatabase("tweets_DB");
	 * 
	 * MongoCollection<Document> collection =
	 * database.getCollection("twitter_tweets");
	 * 
	 * return collection; }
	 */

    private static void run() {
        Logger logger = LoggerFactory.getLogger(TwitterConsumer.class.getName());
  //      MongoCollection<Document> collection = createLink();
        // poll for new data
        String topic = "Twitter-Kafka3";
        final int giveUp = 100;
        int noRecordsCount = 0;
        KafkaConsumer<String, String> consumer = createConsumer("Twitter-Kafka3");
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records) {
                logger.info("consumer record  "+record.toString());
      //          Document doc = Document.parse(record.value());
     //           collection.insertOne(doc);
               
            }
            try {
                consumer.commitSync();
            } catch (CommitFailedException e) {
                logger.info("commit failed", e);
            }
        }
    }

}
