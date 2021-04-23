package twitter_kafka.app;

import twitter_kafka.app.producer.TwitterProducer;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
    	try {
            // 1. First Run Producer to produce Twitter tweets to Kafka Topic
            new TwitterProducer();
            // 2. Uncomment it to run Kafka producer and subscribe to Twitter Kafka Topic
            // new TwitterConsumer();
            // 3. Run Kafka producer, subscribe to Twitter Kafka topic and ingest data into MongoDB database
            // new KafkaConsumerMongoDb();
        }catch(Exception e){
            System.out.println(e.getStackTrace().getClass());
        }
    }
}
