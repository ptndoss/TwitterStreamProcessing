package com.github.ptndoss.kafkasetup;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.log4j.BasicConfigurator;

import java.util.Properties;

public class ProducerDemo {

    public static void main(String[] args) {

        BasicConfigurator.configure();  //For logging
        String BOOTSTRAP_SERVER = "127.0.0.1:9092";
//        Three Steps in creting Producer
    //    1-    Create Producer Properties
    //    2-    Create Producer
    //    3-    Send Data & Flush to push data to consumer


        //Step 1 - Create Producer Properties
        Properties properties = new Properties();
/*         ****Old Way of Initializing Bootstrap server for Producer
        properties.setProperty("bootstrap.server", BOOTSTRAP_SERVER);
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());
*/
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());


        //Step 2 - Create Producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);


        //Create Data(Producer Record) from producer to send
        ProducerRecord<String, String> record = new ProducerRecord<String, String>("mytopic", "Hello From Kafka Producer");

        //Step 3 - Send Data

        producer.send(record);      //--> This is aSync data send
        producer.flush();           // Flush helps in pushing the data to consumer
        producer.close();

    }
}
