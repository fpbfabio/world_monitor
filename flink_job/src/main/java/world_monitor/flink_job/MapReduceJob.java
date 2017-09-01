package world_monitor.flink_job;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.CollectionEnvironment;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

public class MapReduceJob {

	public void run(ArrayList<Tuple3<Long, String, String>> data) {
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<Tuple3<Long, String, String>> dataSet = env.fromCollection(data);
		DataSet<Event> eventSet = dataSet.flatMap(new FlatMapFunction<Tuple3<Long, String, String>, Event>() {

			@Override
			public void flatMap(Tuple3<Long, String, String> sample, Collector<Event> arg1) throws Exception {
				String value = sample.f2;
				Document doc = Jsoup.parse(value);
				Element earthquakesElements = doc.select("span#ctl00_cph_earthquake_details").first();
				Event event = new Event();
				if (earthquakesElements != null) {
					event.type = "earthquake";
					Element subsubmenu = doc.select("div.subsubmenuitem").first();
					if (subsubmenu != null) {
						Element a = subsubmenu.select("a").first();
						if (a != null && a.hasAttr("href")) {
							event.link = a.attr("attr");
						}
					}
					Element summary = doc.select("#ctl00_cph_Summary").first();
					if (summary != null) {
						Elements lis = summary.select("li");
						for (Element li : lis) {
							String ownText = li.ownText();
							if (ownText.contains("within 100km")) {
								Matcher matcher = Pattern.compile("\\d+").matcher(ownText);
								matcher.find();
								event.peopleWithin100km = Integer.parseInt(matcher.group());
							}
						}
					}
					Element p = earthquakesElements.select("p").first();
					if (p != null) {
						Element b = p.select("b").first();
						if (b != null)
							event.humanitarianImpact = b.ownText().toLowerCase();
					}
					Elements lis = earthquakesElements.select("li");
					for (Element element : lis) {
						if (element.ownText().contains("Magnitude")) {
							Element b = element.select("b").first();
							if (b != null) {
								try {
									event.magnitude = Float.parseFloat(b.ownText());
								} catch (NumberFormatException e) {
								}
							}
						} else if (element.ownText().contains("Depth")) {
							Element b = element.select("b").first();
							if (b != null) {
								try {
									event.depht = Float.parseFloat(b.ownText());
								} catch (NumberFormatException e) {
								}
							}
						} else if (element.ownText().contains("Source")) {
							Element a = element.select("a").first();
							if (a != null)
								event.id = a.ownText();
						} else if (element.ownText().contains("Location")) {
							Element b = element.select("b").first();
							if (b != null) {
								try {
									event.lat = Double.parseDouble(b.ownText());
								} catch (NumberFormatException e) {
								}
							}
							b = element.select("b").last();
							if (b != null) {
								try {
									event.lon = Double.parseDouble(b.ownText());
								} catch (NumberFormatException e) {
								}
							}
						} else if (element.ownText().contains("Time (UTC)")) {
							Element b = element.select("b").first();
							if (b != null) {
								try {
									event.utcTime = b.ownText();
									arg1.collect(event);
								} catch (NumberFormatException e) {
								}
							}
						}
					}
				} else {
					Element currentRow = doc.select("tr.currentrow").first();
					if (currentRow != null) {
						Elements tds = currentRow.select("td");
						if (tds.size() == 8) {
							Element subsubmenu = doc.select("div.subsubmenuitem").first();
							if (subsubmenu != null) {
								Element a = subsubmenu.select("a").first();
								if (a != null && a.hasAttr("href")) {
									event.link = a.attr("attr");
								}
							}
							Element head = doc.select("head").first();
							if (head != null) {
								Element title = doc.select("title").first();
								if (title != null) {
									event.id = title.ownText().split(" ")[2];
								}
							}
							Element element = doc.select("div.content1").first();
							if (element != null) {
								Elements ps = element.select("p");
								for (Element p : ps) {
									if (p.ownText().contains("humanitarian impact")) {
										Element b = p.select("b").first();
										if (b != null) {
											event.humanitarianImpact = b.ownText().toLowerCase();
										}
									}
								}
							}
							event.type = "cyclone";
							event.utcTime = tds.get(2).ownText();
							event.category = tds.get(3).ownText();
							Matcher matcher = Pattern.compile("\\d+").matcher(tds.get(4).select("b").first().ownText());
							matcher.find();
							event.windSpeed = Integer.parseInt(matcher.group());
							String populationAffectedByCycloneWinds = tds.get(6).ownText();
							if (populationAffectedByCycloneWinds.contains("no people")) {
								event.populationAffectedByCycloneWinds = 0;
							} else if (!populationAffectedByCycloneWinds.contains("million")){
								matcher = Pattern.compile("\\d+").matcher(tds.get(6).ownText());
								matcher.find();
								event.populationAffectedByCycloneWinds = Integer.parseInt(matcher.group());
							} else {
								populationAffectedByCycloneWinds = populationAffectedByCycloneWinds.replaceAll("[^\\d.]+|\\.(?!\\d)", "").trim();
								event.populationAffectedByCycloneWinds = (int) (Float.parseFloat(populationAffectedByCycloneWinds) * 1000000);
							}
							String[] location = tds.get(7).ownText().split(",");
							event.lat = Double.parseDouble(location[0].trim());
							event.lon = Double.parseDouble(location[1].trim());
							arg1.collect(event);
						}
					}
				}
			}

		});
		eventSet.flatMap(new FlatMapFunction<Event, Event>() {

			@Override
			public void flatMap(Event arg0, Collector<Event> arg1) throws Exception {
				arg1.collect(arg0);
			}
			
		});
		try {
			List<Event> eventList = eventSet.collect();
			for (Event event : eventList) {
				System.out.println(event.toString());
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
