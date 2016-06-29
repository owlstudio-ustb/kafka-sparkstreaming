package careland.kafka;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;

import careland.kafka.util.KafkaUtil;

public class KafkaConsumerTest {

	public static void main(String[] args) throws Exception {
		seekTest();
	}

	/**
	 * 这个是一次性取出来就不管了，kafka认为取出来的数据是肯定能被处理的。所以是consumer.commitSync();，
	 * 这样poll出来的数据认为是全都提交了。
	 * 
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
				System.out.println("fetch data:" + record.value());
				buffer.add(record);
				Thread.sleep(2000);
			}
			if (buffer.size() >= minBatchSize) {
				insertIntoDb(buffer);
				// 这行代码是重点
				consumer.commitSync();
				System.out.println("commit");
				buffer.clear();
			}
		}
	}

	private static void insertIntoDb(List<ConsumerRecord<String, String>> list) {
		// System.out.println("list insert into db");
	}

	/**
	 * 更精细的偏移量控制(控制每个partition的偏移量)。
	 */
	public static void finerOffset() {
		KafkaConsumer<String, String> consumer = KafkaUtil.getConsumer();
		consumer.subscribe(Arrays.asList("test4"));
		try {
			while (true) {
				ConsumerRecords<String, String> records = consumer.poll(Long.MAX_VALUE);
				for (TopicPartition partition : records.partitions()) {
					List<ConsumerRecord<String, String>> partitionRecords = records.records(partition);
					for (ConsumerRecord<String, String> record : partitionRecords) {
						// 处理 逻辑
						System.out.println(partition.toString() + ":" + record.offset() + ": " + record.value());
					}
					long lastOffset = partitionRecords.get(partitionRecords.size() - 1).offset();
					// consumer.commitSync(Collections.singletonMap(partition,
					// new OffsetAndMetadata(lastOffset + 1)));
					// consumer.commitSync(Collections.singletonMap(partition,
					// new OffsetAndMetadata(1)));
					consumer.seek(partition, lastOffset - 1);
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
				if (!closed.get())
					throw e;
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

	public static void seekTest() throws InterruptedException {
		Properties props = new Properties();
		props.put("bootstrap.servers", "Slave01.Hadoop:9092,Slave02.Hadoop:9092,Slave03.Hadoop:9092");
		props.put("group.id", "8");
		props.put("enable.auto.commit", "false");
		// 请参考KafkaConsumer类的说明和例子
		// latest, earliest, none
		props.put("auto.offset.reset", "none");
		// props.put("auto.offset.reset", "earliest");
		props.put("auto.commit.interval.ms", "1000");
		props.put("session.timeout.ms", "30000");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
		String topic = "test4";
		TopicPartition partition0 = new TopicPartition(topic, 0);
		TopicPartition partition1 = new TopicPartition(topic, 1);
		TopicPartition partition2 = new TopicPartition(topic, 2);
		consumer.assign(Arrays.asList(partition0, partition1, partition2));
		try {
			// 从数据库读取每个partition的偏移量
//			long partition0_offset = getLastOffset(partition0);
//			consumer.seek(partition0, partition0_offset);
//			long partition1_offset = getLastOffset(partition1);
//			consumer.seek(partition1, partition1_offset);
//			long partition2_offset = getLastOffset(partition2);
//			consumer.seek(partition2, partition2_offset);
			 consumer.seek(partition0, 55);
			 consumer.seek(partition1, 40);
			 consumer.seek(partition2, 39);
			while (true) {
				ConsumerRecords<String, String> records = consumer.poll(Long.MAX_VALUE);
				for (TopicPartition partition : records.partitions()) {
					List<ConsumerRecord<String, String>> partitionRecords = records.records(partition);
					for (ConsumerRecord<String, String> record : partitionRecords) {
						// 处理 逻辑
						System.out.println(partition.toString() + ":" + record.offset() + ": " + record.value());
					}
					long lastOffset = partitionRecords.get(partitionRecords.size() - 1).offset();
					consumer.commitSync(Collections.singletonMap(partition, new OffsetAndMetadata(lastOffset + 1)));
					// 更新数据库中的偏移量
					updateOffset(partition, lastOffset);
				}
				Thread.sleep(2000);
			}
		} finally {
			consumer.close();
		}
	}

	private static void updateOffset(TopicPartition topicPartition, long offset) {
		System.out.println("Partition:"+topicPartition.partition()+" offset:"+offset);
	}

	private static long getLastOffset(TopicPartition topicPartition) {
		return 0;
	}
}
