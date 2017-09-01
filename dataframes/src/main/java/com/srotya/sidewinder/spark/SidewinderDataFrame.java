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

import java.io.Serializable;
import java.util.Arrays;

import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.sources.BaseRelation;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.sources.PrunedFilteredScan;
import org.apache.spark.sql.sources.PrunedScan;
import org.apache.spark.sql.sources.RelationProvider;
import org.apache.spark.sql.sources.TableScan;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import scala.collection.JavaConversions;
import scala.collection.immutable.HashMap;
import scala.collection.immutable.Map;

public class SidewinderDataFrame extends BaseRelation
		implements RelationProvider, PrunedScan, PrunedFilteredScan, TableScan, Serializable {

	private static final long serialVersionUID = 1L;

	private transient SQLContext context;
	private String dbname;
	private String measurement;
	private String url;
	private String baseUrl;

	protected SidewinderDataFrame() {
	}

	public SidewinderDataFrame(SQLContext context, Map<String, String> configs) {
		this.context = context;
		java.util.Map<String, String> map = JavaConversions.mapAsJavaMap(configs);
		url = map.get("url");
		dbname = map.get("database.name");
		measurement = map.get("measurement.name");
		baseUrl = url + "/databases/" + dbname + "/measurements/" + measurement;
	}

	@Override
	public StructType schema() {
		Metadata md = new Metadata(new HashMap<>());
		StructField[] sf = new StructField[] { 
				new StructField("timestamp", DataTypes.LongType, false, md),
				new StructField("value", DataTypes.LongType, false, md),
				new StructField("valuefield", DataTypes.StringType, false, md),
				new StructField("tags", DataTypes.createArrayType(DataTypes.StringType), false, md),
				new StructField("fp", DataTypes.BooleanType, false, md)
		};

	return new StructType(sf);

	}

	@Override
	public SQLContext sqlContext() {
		return context;
	}

	@Override
	public BaseRelation createRelation(SQLContext ctx, Map<String, String> arg1) {
		return new SidewinderDataFrame(ctx, arg1);
	}

	@Override
	public RDD<Row> buildScan(String[] fields, Filter[] filters) {
		System.out.println("RDD filter:" + Arrays.asList(fields) + "\tFilter:" + Arrays.toString(filters));
		return new SidewinderRDD(context.sparkContext(), url, dbname, measurement, fields, this);
	}

	@Override
	public RDD<Row> buildScan() {
		System.out.println("RDD");
		return new SidewinderRDD(context.sparkContext(), url, dbname, measurement, null, this);
	}

	@Override
	public RDD<Row> buildScan(String[] fields) {
		System.out.println("RDD no filter:" + Arrays.asList(fields));
		return new SidewinderRDD(context.sparkContext(), url, dbname, measurement, fields, this);
	}

}
