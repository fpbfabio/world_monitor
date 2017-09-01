package world_monitor.flink_job;


public class Event {

	public String id;
	public double lat;
	public double lon;
	public String humanitarianImpact;
	public String utcTime;
	public String link;
	public String type;
	public float depht;
	public float magnitude;
	public int peopleWithin100km;
	public String category;
	public int windSpeed;
	public int populationAffectedByCycloneWinds;

	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("id = ");
		builder.append(id);
		builder.append("\n");
		builder.append("gps = ");
		builder.append(lat);
		builder.append(",");
		builder.append(lon);
		builder.append("\n");
		builder.append("Humanitarian impact = ");
		builder.append(humanitarianImpact);
		builder.append("\n");
		builder.append("utcTime = ");
		builder.append(utcTime);
		builder.append("\n");
		builder.append("link = ");
		builder.append(link);
		builder.append("\n");
		builder.append("type = ");
		builder.append(type);
		builder.append("\n");
		builder.append("depht = ");
		builder.append(depht);
		builder.append("\n");
		builder.append("magnitude = ");
		builder.append(magnitude);
		builder.append("\n");
		builder.append("peopleWithin100km = ");
		builder.append(peopleWithin100km);
		builder.append("\n");
		builder.append("category = ");
		builder.append(category);
		builder.append("\n");
		builder.append("windSpeed = ");
		builder.append(windSpeed);
		builder.append("\n");
		builder.append("populationAffectedByCycloneWinds = ");
		builder.append(populationAffectedByCycloneWinds);
		builder.append("\n");
		return builder.toString();
	}
}
