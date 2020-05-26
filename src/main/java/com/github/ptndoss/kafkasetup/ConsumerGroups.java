package com.github.ptndoss.kafkasetup;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ConsumerGroups {

    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(ConsumerGroups.class.getName());
        System.out.println("Consumer");

        //Create Properties

        String BOOTSTRAP_SERVER = "127.0.0.1:9092";
        String GroupID = "My_Fifth_Group";
        String topic = "mytopic";
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,GroupID);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        //Create Consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
        // Subscribe To a topic Messages
        consumer.subscribe(Collections.singleton(topic)); //Subscribe to Single topic

        /*consumer.subscribe(Arrays.asList(topic, "topic2", "topic3")); //Subsribe to multiple topic*/


        while(true){
            ConsumerRecords<String,String>  consumerRecord =  consumer.poll(Duration.ofMillis(100));

            for(ConsumerRecord<String, String> record : consumerRecord){
                logger.info("Key" + record.key() + "Value" + record.value()
                +"Partitio " + record.partition()+
                        "Offser " + record.offset() );
            }
        }


    }
}
