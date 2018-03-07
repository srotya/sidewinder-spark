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
package com.srotya.sidewinder.spark;

import java.io.IOException;
import java.io.Serializable;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.logging.Logger;

import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.util.EntityUtils;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.sources.And;
import org.apache.spark.sql.sources.BaseRelation;
import org.apache.spark.sql.sources.EqualNullSafe;
import org.apache.spark.sql.sources.EqualTo;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.sources.GreaterThan;
import org.apache.spark.sql.sources.GreaterThanOrEqual;
import org.apache.spark.sql.sources.LessThan;
import org.apache.spark.sql.sources.LessThanOrEqual;
import org.apache.spark.sql.sources.Or;
import org.apache.spark.sql.sources.PrunedFilteredScan;
import org.apache.spark.sql.sources.PrunedScan;
import org.apache.spark.sql.sources.RelationProvider;
import org.apache.spark.sql.sources.StringContains;
import org.apache.spark.sql.sources.StringEndsWith;
import org.apache.spark.sql.sources.StringStartsWith;
import org.apache.spark.sql.sources.TableScan;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import com.google.gson.Gson;

import scala.collection.JavaConversions;
import scala.collection.immutable.HashMap;
import scala.collection.immutable.Map;

public class SidewinderDataFrame extends BaseRelation
		implements RelationProvider, PrunedScan, PrunedFilteredScan, TableScan, Serializable {

	private static final Logger logger = Logger.getLogger(SidewinderDataFrame.class.getName());
	private static final long serialVersionUID = 1L;
	private transient SQLContext context;
	private String dbname;
	private String measurement;
	private String url;
	private String baseUrl;
	private String schema;

	protected SidewinderDataFrame() {
	}

	public SidewinderDataFrame(SQLContext context, Map<String, String> configs) {
		this();
		this.context = context;
		java.util.Map<String, String> map = null;
		if (configs != null) {
			map = JavaConversions.mapAsJavaMap(configs);
		} else {
			map = new java.util.HashMap<>();
		}
		url = map.getOrDefault("url", "http://localhost:8080");
		dbname = map.getOrDefault("database.name", "graphite");
		measurement = map.getOrDefault("measurement.name", "threadpool");
		schema = dbname + "." + measurement;
		baseUrl = url + "/databases/" + dbname + "/measurements/" + measurement;
		System.out.println("Constructed data frame source");
	}

	public SidewinderDataFrame(SQLContext context, Map<String, String> configs, StructType struct) {
		this(context, configs);
		System.out.println("Constructed data frame with struct:" + struct);
	}

	public SidewinderDataFrame(SQLContext context, SaveMode saveMode, Map<String, String> configs, DataFrame df) {
		this(context, configs);
		System.out.println("Constructed data frame with df:" + df);
	}

	@Override
	public StructType schema() {
		// query fields from server
		getFieldsFromServer();
		Metadata md = new Metadata(new HashMap<>());
		List<StructField> sf = new ArrayList<>();
		sf.add(new StructField("timestamp", DataTypes.LongType, false, md));
		sf.add(new StructField("tags", DataTypes.createArrayType(DataTypes.StringType), false, md));
		sf.add(new StructField("fp", DataTypes.BooleanType, false, md));
		sf.add(new StructField("value", DataTypes.LongType, false, md));
		sf.add(new StructField("valuefield", DataTypes.StringType, false, md));
		return new StructType(sf.toArray(new StructField[0]));

	}

	private void getFieldsFromServer() {
		HttpGet request = new HttpGet(baseUrl + "/fields");
		try {
			CloseableHttpResponse response = SidewinderRDD.makeRequest(request);
			String str = EntityUtils.toString(response.getEntity());
			Gson gson = new Gson();
			String[] fromJson = gson.fromJson(str, String[].class);
			for (String f : fromJson) {
				logger.info("Found field to for schema(" + schema + ") " + f);
			}
		} catch (KeyManagementException | NoSuchAlgorithmException | KeyStoreException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		logger.info("Updated fields for measurement " + schema);
	}

	@Override
	public SQLContext sqlContext() {
		return context;
	}

	@Override
	public BaseRelation createRelation(SQLContext ctx, Map<String, String> config) {
		return new SidewinderDataFrame(ctx, config);
	}

	@Override
	public RDD<Row> buildScan(String[] fields, Filter[] filters) {
		System.out.println("Scan RDD fields:" + Arrays.asList(fields) + "\tFilter:" + Arrays.toString(filters));
		java.util.Map.Entry<Long, Long> range = extractTimeRange(filters);
		System.out.println("time range:" + range);
		List<String> tags = extractTagFilter(filters);
		System.out.println("tags:" + tags);
		return new SidewinderRDD(context.sparkContext(), url, dbname, measurement, fields, range, this);
	}
	
	@Override
	public RDD<Row> buildScan() {
		System.out.println("Scan RDD");
		return new SidewinderRDD(context.sparkContext(), url, dbname, measurement, null,
				new AbstractMap.SimpleEntry<Long, Long>(0L, Long.MAX_VALUE), this);
	}

	@Override
	public RDD<Row> buildScan(String[] fields) {
		System.out.println("Scan RDD no filter:" + Arrays.asList(fields));
		return new SidewinderRDD(context.sparkContext(), url, dbname, measurement, fields,
				new AbstractMap.SimpleEntry<Long, Long>(0L, Long.MAX_VALUE), this);
	}

	public static java.util.Map.Entry<Long, Long> extractTimeRange(Filter[] filters) {
		Queue<Filter> filterQueue = new ArrayBlockingQueue<>(100);
		for (Filter f : filters) {
			filterQueue.add(f);
		}
		long startTime = 0;
		long endTime = Long.MAX_VALUE;
		while (!filterQueue.isEmpty()) {
			Filter filter = filterQueue.poll();
			String attribute = null;
			String type = null;
			Long value = null;
			if ((filter instanceof EqualTo) || (filter instanceof EqualNullSafe)) {
				if (filter instanceof EqualTo) {
					attribute = ((EqualTo) filter).attribute();
					value = (Long) ((EqualTo) filter).value();
				} else {
					attribute = ((EqualNullSafe) filter).attribute();
					value = (Long) ((EqualNullSafe) filter).value();
				}
				type = "equals";
			} else if (filter instanceof GreaterThan) {
				attribute = ((GreaterThan) filter).attribute();
				value = (Long) ((GreaterThan) filter).value();
				type = "gt";
			} else if (filter instanceof GreaterThanOrEqual) {
				attribute = ((GreaterThanOrEqual) filter).attribute();
				value = (Long) ((GreaterThanOrEqual) filter).value();
				type = "gte";
			} else if (filter instanceof LessThanOrEqual) {
				attribute = ((LessThanOrEqual) filter).attribute();
				value = (Long) ((LessThanOrEqual) filter).value();
				type = "lte";
			} else if (filter instanceof LessThan) {
				attribute = ((LessThan) filter).attribute();
				value = (Long) ((LessThan) filter).value();
				type = "lt";
			} else {
				// ignore
				if (filter instanceof And) {
					filterQueue.add(((And) filter).copy$default$1());
					filterQueue.add(((And) filter).copy$default$2());
				} else if (filter instanceof Or) {
					filterQueue.add(((Or) filter).copy$default$1());
					filterQueue.add(((Or) filter).copy$default$2());
				}
				continue;
			}
			if (attribute.equalsIgnoreCase("timestamp") || attribute.equalsIgnoreCase("time")) {
				// perform time range filter
				switch (type) {
				case "equals":
					startTime = value;
					endTime = startTime;
					return new AbstractMap.SimpleEntry<Long, Long>(startTime, endTime);
				case "gt":
				case "gte":
					startTime = value;
					break;
				case "lt":
				case "lte":
					endTime = value;
					break;
				default:
					throw new UnsupportedOperationException(
							"Timestamp can only be filtered with numeric range filtering");
				}
			} else {
				continue;
			}
		}
		return new AbstractMap.SimpleEntry<Long, Long>(startTime, endTime);
	}

	public static List<String> extractTagFilter(Filter[] filters) {
		List<String> tags = new ArrayList<>();
		Queue<Filter> filterQueue = new ArrayBlockingQueue<>(100);
		for (Filter f : filters) {
			filterQueue.add(f);
		}
		while (!filterQueue.isEmpty()) {
			Filter filter = filterQueue.poll();
			if (filter instanceof And) {
				filterQueue.add(((And) filter).copy$default$1());
				filterQueue.add(((And) filter).copy$default$2());
				continue;
			} else if (filter instanceof Or) {
				filterQueue.add(((Or) filter).copy$default$1());
				filterQueue.add(((Or) filter).copy$default$2());
				continue;
			}
			String attribute = null;
			String type = null;
			String value = null;
			if (filter instanceof EqualTo) {
				attribute = ((EqualTo) filter).attribute();
				value = ((EqualTo) filter).value().toString();
				type = "equals";
			} else if (filter instanceof StringContains) {
				attribute = ((StringContains) filter).attribute();
				value = ((StringContains) filter).value();
				type = "contains";
			} else if (filter instanceof StringStartsWith) {
				attribute = ((StringStartsWith) filter).attribute();
				value = ((StringStartsWith) filter).value();
				type = "starts";
			} else if (filter instanceof StringEndsWith) {
				attribute = ((StringEndsWith) filter).attribute();
				value = ((StringEndsWith) filter).value();
				type = "ends";
			} else {
				// ignore and continue
				continue;
			}
			if (attribute.equalsIgnoreCase("tag") || attribute.equalsIgnoreCase("tags")) {
				switch (type) {
				case "equals":
					// do nothing
					break;
				case "starts":
					value = "^" + value;
					break;
				case "ends":
					value = value + "$";
					break;
				case "contains":
					value = ".*" + value + ".*";
					break;
				default:
					throw new UnsupportedOperationException("Invalid tag filter");
				}
				tags.add(value);
			} else {
				continue;
			}
		}
		return tags;
	}

}
