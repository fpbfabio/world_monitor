package world_monitor.flink_job;

import org.apache.kafka.clients.consumer.*;

import java.util.Arrays;
import java.util.Properties;

public class Consumer {

	private static final String TOPIC_NAME = "event";
	private static final int PORT = 9092;
	private static final long POLL_TIMEOUT = 100;
	private KafkaConsumer<String, String> consumer;
	private Thread consumerThread;
	private ConsumerListener listener;

	private Properties createProperties() {
		Properties properties = new Properties();
		properties.put("bootstrap.servers", "localhost:" + PORT);
		properties.put("group.id", "test");
		properties.put("enable.auto.commit", "true");
		properties.put("auto.commit.interval.ms", "1000");
		properties.put("session.timeout.ms", "30000");
		properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		return properties;
	}

	private void initKafkaConsumer(Properties properties) throws Exception {
		consumer = new KafkaConsumer<String, String>(properties);
		consumer.subscribe(Arrays.asList(TOPIC_NAME));
	}

	private void startConsuming() throws Exception {
		stopConsuming();
		consumerThread = new Thread(new ConsumerThread());
		consumerThread.start();
	}

	private void stopConsuming() throws Exception {
		if (consumerThread != null) {
			consumerThread.interrupt();
			consumerThread.join();
		}
	}

	private void waitConsuming() throws Exception {
		if (consumerThread != null) {
			consumerThread.join();
		}
	}

	public void run(ConsumerListener listener) throws Exception {
		Log.i("Starting kafka consumer..");
		setListener(listener);
		Properties properties = createProperties();
		initKafkaConsumer(properties);
		startConsuming();
		waitConsuming();
	}

	private void setListener(ConsumerListener listener) {
		this.listener = listener;
	}

	public interface ConsumerListener {
		void onDataReceived(Record[] records);
	}

	public class Record {
		private long offset;
		private String key;
		private String value;

		public Record(long offset, String key, String value) {
			setOffset(offset);
			setKey(key);
			setValue(value);
		}

		public long getOffset() {
			return offset;
		}

		public void setOffset(long offset) {
			this.offset = offset;
		}

		public String getKey() {
			return key;
		}

		public void setKey(String key) {
			this.key = key;
		}

		public String getValue() {
			return value;
		}

		public void setValue(String value) {
			this.value = value;
		}
	}

	private class ConsumerThread implements Runnable {

		public void run() {
			Log.i("Starting polling..");
			while (!Thread.currentThread().isInterrupted()) {
				ConsumerRecords<String, String> consumerRecords = consumer.poll(POLL_TIMEOUT);
				if (consumerRecords.count() > 0) {
					Record[] records = new Record[consumerRecords.count()];
					int i = 0;
					for (ConsumerRecord<String, String> record : consumerRecords)
						records[i++] = new Record(record.offset(), record.key(), record.value());
					listener.onDataReceived(records);
				}
			}
		}
	}
}
