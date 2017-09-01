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
package com.srotya.sidewinder.spark.example;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

import com.srotya.sidewinder.spark.SidewinderDataFrame;

public class SparkSidewinderExample {

	public static void main(String[] args) {
		JavaSparkContext ctx = new JavaSparkContext("local[*]", "sql-test");
		
		
		JavaRDD<Object> rdd = ctx.emptyRDD();
		JavaRDD<String> map = rdd.map(new Function<Object, String>() {

			private static final long serialVersionUID = 1L;

			@Override
			public String call(Object v1) throws Exception {
				// TODO Auto-generated method stub
				return null;
			}
		});
		SQLContext sqlContext = new SQLContext(ctx);

		DataFrame df = sqlContext.baseRelationToDataFrame(
				new SidewinderDataFrame(sqlContext, new scala.collection.immutable.HashMap<String, String>()));
		df.registerTempTable("root");
		sqlContext.sql("select tags from root where value>5 and value<10").show();
		ctx.close();
	}

}