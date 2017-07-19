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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.Partition;
import org.apache.spark.SparkContext;
import org.apache.spark.TaskContext;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRow;

import scala.collection.Iterator;
import scala.collection.JavaConversions;

/**
 * @author ambud
 */
public final class SidewinderRDD extends RDD<Row> {
	
	private static final long serialVersionUID = 1L;

	public SidewinderRDD(SparkContext _sc) {
		super(_sc, JavaConversions.asScalaBuffer(Arrays.asList()), scala.reflect.ClassTag$.MODULE$.apply(Row.class));
	}

	@Override
	public Partition[] getPartitions() {
		return new Partition[] { new Partition() {

			private static final long serialVersionUID = 1L;

			@Override
			public int index() {
				return 0;
			}
		} };
	}

	@Override
	public Iterator<Row> compute(Partition arg0, TaskContext ctx) {
		List<Row> objs = new ArrayList<>();
		for (int i = 0; i < 100; i++) {
			objs.add(new GenericRow(new Object[] { "cpu1", "valu1", new String[] { "testtes2" },
					System.currentTimeMillis(), 1L, false }));
		}
		return JavaConversions.asScalaIterator(objs.iterator());
	}
}