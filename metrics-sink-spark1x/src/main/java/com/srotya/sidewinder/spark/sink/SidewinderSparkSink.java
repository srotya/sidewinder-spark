/**
 * Copyright 2017 Ambud Sharma
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * 		http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.srotya.sidewinder.spark.sink;

import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.methods.StringRequestEntity;
import org.apache.spark.SecurityManager;
import org.apache.spark.metrics.MetricsSystem;
import org.apache.spark.metrics.sink.Sink;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;
import com.codahale.metrics.Timer;

/**
 * 
 * @author ambud
 */
public class SidewinderSparkSink implements Sink {

	private static final String DB_URL = "url";
	private static final String POLL_DURATION = "poll.duration";
	private SidewinderReporter sidewinderReporter;
	private int period;
	private String url;

	public SidewinderSparkSink(Properties props, MetricRegistry registry, SecurityManager manager) {
		url = props.getProperty(DB_URL);
		period = Integer.parseInt(props.getOrDefault(POLL_DURATION, "5").toString());
		MetricsSystem.checkMinimalPollingPeriod(TimeUnit.SECONDS, period);
		sidewinderReporter = new SidewinderReporter(registry, "spark", MetricFilter.ALL, TimeUnit.SECONDS,
				TimeUnit.MILLISECONDS, url, props);
	}

	@Override
	public void report() {
		sidewinderReporter.report();
	}

	@Override
	public void start() {
		sidewinderReporter.start(period, TimeUnit.SECONDS);
	}

	@Override
	public void stop() {
		sidewinderReporter.close();
	}

	public static class SidewinderReporter extends ScheduledReporter {

		private static final Logger logger = Logger.getLogger(SidewinderReporter.class.getName());
		private String url;

		protected SidewinderReporter(MetricRegistry registry, String name, MetricFilter filter, TimeUnit rateUnit,
				TimeUnit durationUnit, String url, Properties props) {
			super(registry, name, filter, rateUnit, durationUnit);
			this.url = url;
		}

		@SuppressWarnings("rawtypes")
		@Override
		public void report(SortedMap<String, Gauge> gauges, SortedMap<String, Counter> counters,
				SortedMap<String, Histogram> histograms, SortedMap<String, Meter> meters,
				SortedMap<String, Timer> timers) {
			long ts = System.currentTimeMillis() * 1000 * 1000;
			StringBuilder builder = new StringBuilder();
			for (Entry<String, Gauge> entry : gauges.entrySet()) {
				String k = entry.getKey();
				Object value = entry.getValue().getValue();
				extracted(ts, builder, k, "gauge", value);
			}
			for (Entry<String, Counter> entry : counters.entrySet()) {
				String k = entry.getKey();
				Object value = entry.getValue().getCount();
				extracted(ts, builder, k, "counter", value);
			}
			for (Entry<String, Histogram> entry : histograms.entrySet()) {
				String k = entry.getKey();
				Object value = entry.getValue().getCount();
				extracted(ts, builder, k, "histogram", value);
			}
			for (Entry<String, Meter> entry : meters.entrySet()) {
				String k = entry.getKey();
				Object value = entry.getValue().getCount();
				extracted(ts, builder, k, "meter", value);
			}
			for (Entry<String, Timer> entry : timers.entrySet()) {
				String k = entry.getKey();
				Object value = entry.getValue().getCount();
				extracted(ts, builder, k, "timer", value);
			}
			try {
				String payload = builder.toString().trim();
				if (payload.length() == 0) {
					System.out.println("Empty metrics request ignoring");
					return;
				}
				int response = putData(url, payload);
				System.out.println("\n\nResponse:" + response);
				if (response == 400) {
					System.out.println("Bad data:\n" + payload);
				}
			} catch (Exception e) {
				logger.log(Level.SEVERE, "Error pushing metrics to Sidewinder", e);
			}
		}

		public static void extracted(long ts, StringBuilder builder, String type, String k, Object value) {
			String[] key = k.split("\\.");
			String appId = key[0];
			String component = key[1];
			String valueField = key[key.length - 1];
			List<String> tags = new ArrayList<>();
			tags.add(appId);
			tags.add(type);
			int i = 2;
			if (!(component.equalsIgnoreCase("driver") || component.equalsIgnoreCase("executor"))) {
				tags.add("executor_id=" + component);
				component = key[2];
				i = 3;
			}
			builder.append(component);
			for (String tag : tags) {
				builder.append("," + tag);
			}
			for (; i < key.length - 1; i++) {
				builder.append("," + key[i]);
			}
			if ((value instanceof Double) || (value instanceof Float)) {
				builder.append(" " + valueField + "=" + value);
			} else {
				builder.append(" " + valueField + "=" + value + "i");
			}
			builder.append(" " + ts + "\n");
		}

		public static int putData(String url, String data) throws Exception {
			HttpClient client = new HttpClient();
			PostMethod method = new PostMethod(url);
			method.setRequestEntity(new StringRequestEntity(data, "text/html", "utf-8"));
			return client.executeMethod(method);
		}

	}

}
