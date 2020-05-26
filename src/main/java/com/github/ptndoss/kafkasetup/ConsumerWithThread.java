package com.github.ptndoss.kafkasetup;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerWithThread {

    public static void main(String[] args) {
 /*       Logger logger = LoggerFactory.getLogger(ConsumerWithThread.class.getName());
        System.out.println("Consumer");

        //Create Properties

        String BOOTSTRAP_SERVER = "127.0.0.1:9092";
        String GroupID = "My_Consumer_Group";
        String topic = "mytopic";
        Properties properties = new Properties();

        //Create Consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
        // Subscribe To a topic Messages
        consumer.subscribe(Collections.singleton(topic)); //Subscribe to Single topic

        *//*consumer.subscribe(Arrays.asList(topic, "topic2", "topic3")); //Subsribe to multiple topic*//*


    */
        new ConsumerWithThread().run();

    }

    private ConsumerWithThread(){
        run();
    }

    private void run(){
        Logger logger = LoggerFactory.getLogger(ConsumerWithThread.class.getName());
        String BOOTSTRAP_SERVER = "127.0.0.1:9092";
        String GroupID = "My_Consumer_Group";
        String topic = "mytopic";
        CountDownLatch latch = new CountDownLatch(1);
        logger.info("Creating Consmer Thread");
        Runnable myConsumerRunnable = new ConsumerThreadRunnable(topic, BOOTSTRAP_SERVER, GroupID, latch);

        Thread myRunnableThread = new Thread(myConsumerRunnable);
        myRunnableThread.start();
        try {
            latch.wait();
        } catch (InterruptedException e) {
            logger.info("Application gets interrupt");
            e.printStackTrace();
        }finally{
            logger.info("Application is closing");
        }

    }
    public class ConsumerThreadRunnable implements Runnable{
        private CountDownLatch latch;
        private KafkaConsumer<String, String> consumer;
        private Properties properties;
        Logger logger;
        public ConsumerThreadRunnable(String topic, String BOOTSTRAP_SERVER, String GroupID,
                                      CountDownLatch latch){
            logger = LoggerFactory.getLogger(ConsumerWithThread.class.getName());
            this.latch = latch;
            properties = new Properties();
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,GroupID);
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

            consumer = new KafkaConsumer<String, String>(properties);
            consumer.subscribe(Collections.singleton(topic)); //Subscribe to Single topic

        }

        @Override
        public void run() {
            try{

                while(true){
                    ConsumerRecords<String,String>  consumerRecord =  consumer.poll(Duration.ofMillis(100));

                    for(ConsumerRecord<String, String> record : consumerRecord){
                        logger.info("Key" + record.key() + "Value" + record.value()
                                +"Partitio " + record.partition()+
                                "Offser " + record.offset() );
                    }
                }
            }catch(WakeupException e){
                logger.info("Recieved Shutdown" + e.getMessage());
            }finally {
                consumer.close();
                latch.countDown();      // Inform main code to stop processing
            }
        }

        public void shutdown(){
            consumer.wakeup(); // Special method for interrupting consumer.poll()
        }
    }
}
