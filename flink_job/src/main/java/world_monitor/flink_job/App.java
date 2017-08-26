package world_monitor.flink_job;

import world_monitor.flink_job.Consumer.Record;
import world_monitor.flink_job.Consumer.ConsumerListener;


public class App {

	public static void main(String[] args) throws Exception {
		Consumer consumer = new Consumer();
		consumer.run(new ConsumerListener() {

			public void onDataReceived(Record[] records) {
				for (Record r : records)
					System.out.println(r.getValue());
			}
		});
	}
}
