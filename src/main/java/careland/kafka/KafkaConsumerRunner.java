package careland.kafka;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;

/**
 * Hello world!
 *
 */
@SuppressWarnings("rawtypes")
public class KafkaConsumerRunner implements Runnable {
    private final AtomicBoolean closed = new AtomicBoolean(false);
	private final KafkaConsumer consumer;

    public KafkaConsumerRunner(KafkaConsumer consumer){
    	this.consumer = consumer;
    }
    public void run() {
        try {
       //     consumer.subscribe(Arrays.asList("topic"));
            while (!closed.get()) {
                ConsumerRecords<String,String> records = consumer.poll(10000);
                for(ConsumerRecord<String,String> record : records){
                	System.out.println("线程:"+Thread.currentThread().getName()+" 分区："+record.partition() + " 偏移量：" + record.offset() + " 值:" + record.value());
                }
                System.out.println("....");  
                Thread.sleep(2000);
            }
        } catch (WakeupException e) {
            // Ignore exception if closing
            if (!closed.get()) throw e;
        } catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
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
