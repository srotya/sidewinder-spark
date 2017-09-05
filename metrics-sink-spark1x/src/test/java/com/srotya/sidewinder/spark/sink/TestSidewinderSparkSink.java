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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.List;

import org.junit.Test;

import com.srotya.sidewinder.core.storage.DataPoint;
import com.srotya.sidewinder.core.utils.HTTPDataPointDecoder;
import com.srotya.sidewinder.spark.sink.SidewinderSparkSink.SidewinderReporter;

/**
 * @author ambud
 */
public class TestSidewinderSparkSink {

	@Test
	public void testSidewinderReporter() {
		long ts = System.currentTimeMillis();
		StringBuilder builder = new StringBuilder();
		String k = "application_1503944613984_0005.driver.jvm.pools.PS-Eden-Space.usage";
		SidewinderReporter.extracted(ts * 1000 * 1000, builder, "gauge", k, 0.123123);
		List<DataPoint> points = HTTPDataPointDecoder.dataPointsFromString("db1", builder.toString());
		assertEquals(1, points.size());
		DataPoint dp = points.get(0);
		assertEquals("driver", dp.getMeasurementName());
		assertEquals("usage", dp.getValueFieldName());
		assertEquals(true, dp.isFp());
		assertEquals(0.123123, dp.getValue(), 0.001);
		assertEquals(ts, dp.getTimestamp());
		assertTrue(dp.getTags().contains("application_1503944613984_0005"));
		assertTrue(dp.getTags().contains("jvm"));
		assertTrue(dp.getTags().contains("pools"));
		assertTrue(dp.getTags().contains("PS-Eden-Space"));
		assertTrue(dp.getTags().contains("gauge"));

		builder = new StringBuilder();
		k = "application_1503944613984_0005.1.executor.jvm.pools.PS-Eden-Space.usage1";
		SidewinderReporter.extracted(ts, builder, "meter", k, 32343);
		points = HTTPDataPointDecoder.dataPointsFromString("db1", builder.toString());
		assertEquals(1, points.size());
		dp = points.get(0);
		assertEquals("executor", dp.getMeasurementName());
		assertEquals("usage1", dp.getValueFieldName());
		assertEquals(false, dp.isFp());
		assertEquals(32343, dp.getLongValue(), 0.001);
		assertEquals(ts, dp.getTimestamp());
		assertTrue(dp.getTags().contains("application_1503944613984_0005"));
		assertTrue(dp.getTags().contains("jvm"));
		assertTrue(dp.getTags().contains("pools"));
		assertTrue(dp.getTags().contains("PS-Eden-Space"));
		assertTrue(dp.getTags().contains("executor_id=1"));

		builder = new StringBuilder();
		k = "application_1503944613984_0005.executor.jvm.pools.space.usage1";
		SidewinderReporter.extracted(ts, builder, "test", k, 32343);
		assertEquals(1, points.size());
		points = HTTPDataPointDecoder.dataPointsFromString("db1", builder.toString());
		dp = points.get(0);
		assertEquals("executor", dp.getMeasurementName());
		assertEquals("usage1", dp.getValueFieldName());
		assertEquals(false, dp.isFp());
		assertEquals(32343, dp.getLongValue(), 0.001);
		assertEquals(ts, dp.getTimestamp());
		assertTrue(dp.getTags().contains("application_1503944613984_0005"));
		assertTrue(dp.getTags().contains("jvm"));
		assertTrue(dp.getTags().contains("pools"));
		assertTrue(dp.getTags().contains("space"));
	}

}
