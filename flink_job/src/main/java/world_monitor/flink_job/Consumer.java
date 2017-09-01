package world_monitor.flink_job;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Properties;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

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
		void onDataReceived(ArrayList<Tuple3<Long, String, String>> records);
	}

	private class ConsumerThread implements Runnable {

		public void run() {
			Log.i("Starting polling..");
			while (!Thread.currentThread().isInterrupted()) {
				ConsumerRecords<String, String> consumerRecords = consumer.poll(POLL_TIMEOUT);
				if (consumerRecords.count() > 0) {
					ArrayList<Tuple3<Long, String, String>> records = new ArrayList<Tuple3<Long, String, String>>();
					int i = 0;
					for (ConsumerRecord<String, String> record : consumerRecords)
						records.add(new Tuple3<Long, String, String>(record.offset(), record.key(), record.value()));
					listener.onDataReceived(records);
				}
			}
		}
	}
}
