package world_monitor.kafka_producer;


import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;


public class Producer {

	private static final String TOPIC_NAME = "event";
	private static final int PORT = 9092;
	private static final int SOCKET_PORT = 3001;
	private ServerSocket serverSocket;
	private KafkaProducer<String, String> producer;
	private Thread serverThread;

	public void run() throws Exception {
		Log.i("Starting kafka producer in port " + SOCKET_PORT + ", publishing to topic " + TOPIC_NAME);
		Properties properties = createProperties();
		initProducer(properties);
	    initSocket();
	    startServer();
	    serverThread.join();
	}

	private void initSocket() throws Exception {
		serverSocket = new ServerSocket(SOCKET_PORT);
	}

	private void closeSocket() throws Exception {
		serverSocket.close();
	}

	private void startServer() throws Exception {
		stopServer();
		serverThread = new Thread(new ServerThread());
		serverThread.start();
	}

	public void stopServer() throws Exception {
		if (serverThread != null) {
			serverSocket.close();
			serverThread.interrupt();
			serverThread.join();
			serverThread = null;
			serverSocket = null;
		}
	}

	private void initProducer(Properties properties) {
		producer = new KafkaProducer<String, String>(properties);
	}

	private Properties createProperties() throws Exception {
		Properties properties = new Properties();
		properties.put("bootstrap.servers", "localhost:" + PORT);
		properties.put("acks", "all");
		properties.put("retries", 0);
		properties.put("batch.size", 16384);
		properties.put("linger.ms", 1);
		properties.put("buffer.memory", 33554432);
		properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		return properties;
	}

	private String generateRecordId() {
		return Long.toString(System.currentTimeMillis());
	}

	private void sendDataToKafka(String data) {
		String id = generateRecordId();
		producer.send(new ProducerRecord<String, String>(TOPIC_NAME, id, data));
	}

	private class ServerThread implements Runnable {

		public void run() {
			while (!Thread.currentThread().isInterrupted()) {
				try {
					Log.i("Waiting for connection..");
					Socket connectionSocket = serverSocket.accept();
					Thread thread = new Thread(new ConnectionThread(connectionSocket));
					thread.start();
					Log.i("Connection accepted.. reading data");
				} catch (Exception e) {
				}
			}
		}
	}

	private class ConnectionThread implements Runnable {

		private Socket connectionSocket;

		public ConnectionThread(Socket connectionSocket) {
			this.connectionSocket = connectionSocket;
		}

		public void run() {
			try {
				BufferedReader br = new BufferedReader(new InputStreamReader(connectionSocket.getInputStream()));
				StringBuilder builder = new StringBuilder();
				String temp = null;
				while ((temp = br.readLine()) != null) {
					builder.append(temp);
				}
				String data = builder.toString();
				Log.i("Read complete.. data length = " + data.length());
				Log.i("Closing connection..");
				connectionSocket.close();
				Log.i("Sending to kafka now.. ");
				sendDataToKafka(data);
			} catch (Exception e) {

			}
		}
	}
}
