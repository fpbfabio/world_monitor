package world_monitor.flink_job;

import java.util.ArrayList;

import org.apache.flink.api.java.tuple.Tuple3;

import world_monitor.flink_job.Consumer.ConsumerListener;


public class App {

	public static void main(String[] args) throws Exception {
		Consumer consumer = new Consumer();
		final MapReduceJob job = new MapReduceJob();
		consumer.run(new ConsumerListener() {

			public void onDataReceived(ArrayList<Tuple3<Long, String, String>> records) {
				job.run(records);
			}
		});
	}
}
