package world_monitor.kafka_producer;


public class App {

	public static void main(String[] args) throws Exception{
		Producer producer = new Producer();
		producer.run();
	}
}
