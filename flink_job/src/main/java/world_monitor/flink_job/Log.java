package world_monitor.flink_job;

public final class Log {

	private static final boolean PRINT_LOGS = true;

	public static void i(String s) {
		if (PRINT_LOGS)
			System.out.println(s);
	}
}
