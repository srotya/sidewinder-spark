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

import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.sources.BaseRelation;
import org.apache.spark.sql.sources.CreatableRelationProvider;
import org.apache.spark.sql.sources.RelationProvider;
import org.apache.spark.sql.sources.SchemaRelationProvider;
import org.apache.spark.sql.types.StructType;

import scala.collection.immutable.Map;

/**
 * To support sqlContext.load("com.srotya.sidewinder.spark", map)
 * 
 * @author ambud
 */
public class DefaultSource implements RelationProvider, SchemaRelationProvider, CreatableRelationProvider {

	@Override
	public BaseRelation createRelation(SQLContext arg0, Map<String, String> arg1) {
		return new SidewinderDataFrame(arg0, arg1);
	}

	@Override
	public BaseRelation createRelation(SQLContext arg0, Map<String, String> arg1, StructType arg2) {
		return new SidewinderDataFrame(arg0, arg1);
	}

	@Override
	public BaseRelation createRelation(SQLContext arg0, SaveMode arg1, Map<String, String> arg2, DataFrame arg3) {
		return new SidewinderDataFrame(arg0, arg2);
	}

}
