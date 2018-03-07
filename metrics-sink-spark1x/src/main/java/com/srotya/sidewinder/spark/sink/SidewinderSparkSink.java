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

import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.spark.SecurityManager;
import org.apache.spark.metrics.sink.Sink;

import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;

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
		try {
			sidewinderReporter = new SidewinderReporter(registry, "spark", MetricFilter.ALL, TimeUnit.SECONDS,
					TimeUnit.MILLISECONDS, url, props);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void report() {
		sidewinderReporter.report();
		System.out.println("report");
	}

	@Override
	public void start() {
		sidewinderReporter.start(period, TimeUnit.SECONDS);
	}

	@Override
	public void stop() {
		sidewinderReporter.close();
	}

}
