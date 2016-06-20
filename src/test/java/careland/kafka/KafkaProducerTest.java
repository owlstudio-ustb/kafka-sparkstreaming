package careland.kafka;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import careland.kafka.util.KafkaUtil;

public class KafkaProducerTest {
	
    public static void main(String[] args) throws Exception{  
        Producer<String, String> producer = KafkaUtil.getProducer();  
        int i = 130;  
        while(i<140) {  
            ProducerRecord<String, String> record = new ProducerRecord<String, String>("test4", String.valueOf(i), String.valueOf(i));  
            producer.send(record, new Callback() {  
                public void onCompletion(RecordMetadata metadata, Exception e) {  
                    if (e != null)  
                        e.printStackTrace();  
                  //  System.out.print("success send");
                    System.out.println("message send to partition " + metadata.partition() + ", offset: " + metadata.offset());  
                }  
            });
            System.out.println(i);
            i++;  
            Thread.sleep(2000);  
        }  
    }  
}
