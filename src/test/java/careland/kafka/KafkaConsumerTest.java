package careland.kafka;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;

import careland.kafka.util.KafkaUtil;

public class KafkaConsumerTest {
	
    public static void main(String[] args) throws Exception{  
    	finerOffset();
    }
    /**
     * 这个是一次性取出来就不管了，kafka认为取出来的数据是肯定能被处理的。所以是consumer.commitSync();，这样poll出来的数据认为是全都提交了。
     * @throws InterruptedException
     */
	public static void consumer() throws InterruptedException {
		KafkaConsumer<String, String> consumer = KafkaUtil.getConsumer();  
        consumer.subscribe(Arrays.asList("test4")); 
        final int minBatchSize = 2;
        List<ConsumerRecord<String, String>> buffer = new ArrayList<>();
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
            	System.out.println("fetch data:"+record.value());
                buffer.add(record);
                Thread.sleep(2000);  
                if (buffer.size() >= minBatchSize) {
                	insertIntoDb(buffer);
                	//这行代码是重点
                	consumer.commitSync();
                	System.out.println("commit");
                	buffer.clear();
                }
            }
        }
	} 
    
    private static void insertIntoDb(List<ConsumerRecord<String, String>> list){
    	//System.out.println("list insert into db");
    }
    
    /**
     * 更精细的偏移量控制(控制每个partition的偏移量)。
     */
    public static void finerOffset(){
    	 KafkaConsumer<String, String> consumer = KafkaUtil.getConsumer();  
         consumer.subscribe(Arrays.asList("test4")); 
    	try {
            while(true) {
                ConsumerRecords<String, String> records = consumer.poll(Long.MAX_VALUE);
                for (TopicPartition partition : records.partitions()) {
                    List<ConsumerRecord<String, String>> partitionRecords = records.records(partition);
                    for (ConsumerRecord<String, String> record : partitionRecords) {
                    	//处理 逻辑
                        System.out.println(partition.toString()+":"+record.offset() + ": " + record.value());
                    }
                    long lastOffset = partitionRecords.get(partitionRecords.size() - 1).offset();
                  //  consumer.commitSync(Collections.singletonMap(partition, new OffsetAndMetadata(lastOffset + 1)));
                    consumer.commitSync(Collections.singletonMap(partition, new OffsetAndMetadata(1)));
                }
            }
        } finally {
          consumer.close();
        }
    }
    
    public class KafkaConsumerRunner implements Runnable {
        private final AtomicBoolean closed = new AtomicBoolean(false);
        private final KafkaConsumer consumer = KafkaUtil.getConsumer();
        public void run() {
            try {
                consumer.subscribe(Arrays.asList("topic"));
                while (!closed.get()) {
                    ConsumerRecords records = consumer.poll(10000);
                    // Handle new records
                }
            } catch (WakeupException e) {
                // Ignore exception if closing
                if (!closed.get()) throw e;
            } finally {
                consumer.close();
            }
        }

        // Shutdown hook which can be called from a separate thread
        public void shutdown() {
            closed.set(true);
            consumer.wakeup();
        }
    }
}
