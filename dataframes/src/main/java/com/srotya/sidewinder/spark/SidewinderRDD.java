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
import java.net.MalformedURLException;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map.Entry;

import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.spark.Partition;
import org.apache.spark.SparkContext;
import org.apache.spark.TaskContext;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRow;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.StructType;

import scala.collection.Iterator;
import scala.collection.JavaConversions;

/**
 * @author ambud
 */
public final class SidewinderRDD extends RDD<Row> {

	private static final long serialVersionUID = 1L;
	private String baseUrl;
	private SidewinderDataFrame schemaProvider;
	private String[] fields;
	private Entry<Long, Long> timeRangeFilter;

	public SidewinderRDD(SparkContext _sc, String url, String dbname, String measurement, String[] fields,
			java.util.Map.Entry<Long, Long> timeRangeFilter, SidewinderDataFrame schemaProvider) {
		super(_sc, JavaConversions.asScalaBuffer(Arrays.asList()), scala.reflect.ClassTag$.MODULE$.apply(Row.class));
		this.fields = fields;
		this.timeRangeFilter = timeRangeFilter;
		this.schemaProvider = schemaProvider;
		baseUrl = url + "/databases/" + dbname + "/measurements/" + measurement;
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
		StructType schema = schemaProvider.schema();
		System.out.println("Schema:" + schema);
		// get data

		List<Row> objs = new ArrayList<>();
		for (long i = 0; i < 100; i++) {
			GenericRowWithSchema row = new GenericRowWithSchema();
			objs.add(new GenericRow(
					new Object[] { System.currentTimeMillis(), new String[] { "testtes2" }, false, 13212L, "value" }));
		}
		return JavaConversions.asScalaIterator(objs.iterator());
	}

	public static CloseableHttpResponse makeRequest(HttpRequestBase request) throws KeyManagementException,
			ClientProtocolException, NoSuchAlgorithmException, KeyStoreException, MalformedURLException, IOException {
		return buildClient(request.getURI().toURL().toString(), 1000, 1000, null).execute(request);
	}

	public static CloseableHttpResponse makeRequestAuthenticated(HttpRequestBase request, CredentialsProvider provider)
			throws KeyManagementException, ClientProtocolException, NoSuchAlgorithmException, KeyStoreException,
			MalformedURLException, IOException {
		return buildClient(request.getURI().toURL().toString(), 1000, 1000, provider).execute(request);
	}

	public static CloseableHttpClient buildClient(String baseURL, int connectTimeout, int requestTimeout,
			CredentialsProvider provider) throws NoSuchAlgorithmException, KeyStoreException, KeyManagementException {
		HttpClientBuilder clientBuilder = HttpClients.custom();
		if (provider != null) {
			clientBuilder.setDefaultCredentialsProvider(provider);
		}
		RequestConfig config = RequestConfig.custom().setConnectTimeout(connectTimeout)
				.setConnectionRequestTimeout(requestTimeout).setAuthenticationEnabled(true).build();
		return clientBuilder.setDefaultRequestConfig(config).build();
	}
}