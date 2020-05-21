package com.github.ptndoss.kafkasetup;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.log4j.BasicConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallBack {

    public static void main(String[] args) {

        final Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallBack.class);
//        BasicConfigurator.configure();  //For logging
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

        int i = 0;
        while(i < 10){
            String topic = "mytopic";
            String val = "Values " + Integer.toString(i);
            String key = "Id_"+ Integer.toString(i);
            //Create Data(Producer Record) from producer to send
            ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic,key,val);

            //Step 3 - Send Data
            //--> This is aSync data send
            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    //Execute everytime a new record is sent. IF Successfull metadata will have values else exception will have value
                    if(e == null){
                        logger.info("Received new Metadata \n" +
                                "Topic :" + recordMetadata.topic() + "\n"+
                                "Partition :"+recordMetadata.partition() +"\n"+
                                "Offset :" + recordMetadata.offset() +"\n"+
                                "Timestamp : "+recordMetadata.timestamp()+"\n"
                        );
                    }else{
                        logger.info(e.getMessage());
                    }

                }
            });
            i++;
        }
        producer.flush();           // Flush helps in pushing the data to consumer
        producer.close();

    }
}
