package careland.kafka.util;

import java.util.Properties;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;

public class KafkaUtil {
    private static KafkaProducer<String, String> kp;  
    private static KafkaConsumer<String, String> kc; 
    
    public static KafkaProducer<String, String> getProducer() {  
        if (kp == null) {  
            Properties props = new Properties();  
            props.put("bootstrap.servers", "Slave01.Hadoop:9092,Slave02.Hadoop:9092,Slave03.Hadoop:9092");  
            props.put("acks", "1");  
            props.put("retries", 0);  
            props.put("batch.size", 16384); 

            props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");  
            props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");  
            kp = new KafkaProducer<String, String>(props);  
        }  
        return kp;  
    }  
    
    public static KafkaConsumer<String, String> getConsumer() {  
        Properties props = new Properties();  
        props.put("bootstrap.servers", "Slave01.Hadoop:9092,Slave02.Hadoop:9092,Slave03.Hadoop:9092");  
        props.put("group.id", "4");  
        props.put("enable.auto.commit", "false");
        //请参考KafkaConsumer类的说明和例子
       //latest, earliest, none
        props.put("auto.offset.reset", "latest");
       // props.put("auto.offset.reset", "earliest");
        props.put("auto.commit.interval.ms", "1000");  
        props.put("session.timeout.ms", "30000");  
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");  
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");  
        return new KafkaConsumer<String, String>(props);  
    } 
}
