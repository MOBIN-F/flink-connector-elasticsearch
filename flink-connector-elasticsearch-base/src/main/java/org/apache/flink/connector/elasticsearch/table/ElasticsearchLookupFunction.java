/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.elasticsearch.table;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.connector.elasticsearch.sink.NetworkClientConfig;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.conversion.DataStructureConverter;
import org.apache.flink.table.data.conversion.DataStructureConverters;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.LookupFunction;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.types.Row;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** A lookup function for {@link ElasticsearchDynamicSource}. */
@Internal
public class ElasticsearchLookupFunction extends LookupFunction {

    private static final long serialVersionUID = 2L;

    protected RestHighLevelClient client;
    private final Function<RowData, String> createKey;
    private final IndexGenerator indexGenerator;
    private final RowType rowType;
    private final List<HttpHost> hosts;
    private final String[] keyNames;
    private final NetworkClientConfig networkClientConfig;
    private final DeserializationSchema<RowData> format;
    private final DataStructureConverter<Object, Object> converter;


    private SearchRequest request;
    private SearchSourceBuilder searchSourceBuilder;

    public ElasticsearchLookupFunction(
            List<HttpHost> hosts,
            NetworkClientConfig networkClientConfig,
            IndexGenerator indexGenerator,
            Function<RowData, String> createKey,
            String[] keyNames,
            RowType rowType,
            RowType keyRowType,
            DeserializationSchema<RowData> format
    ) {
        checkNotNull(keyNames, "No keyNames supplied.");
        this.hosts = hosts;
        this.networkClientConfig = networkClientConfig;
        this.indexGenerator = checkNotNull(indexGenerator);
        this.createKey = checkNotNull(createKey);
        this.keyNames = keyNames;
        this.rowType = rowType;
        this.format = format;
        this.converter = DataStructureConverters.getConverter(
                TypeConversions.fromLogicalToDataType(keyRowType));
    }

    @Override
    public void open(FunctionContext context) throws Exception {
        super.open(context);
        this.client =
                new RestHighLevelClient(
                        configureRestClientBuilder(
                                RestClient.builder(this.hosts.toArray(new HttpHost[0])),
                                networkClientConfig));
        format.open(null);
        indexGenerator.open();
        request = new SearchRequest();
        searchSourceBuilder = new SearchSourceBuilder();
        String[] fields = rowType.getFieldNames().toArray(new String[rowType.getFieldCount()]);
        FetchSourceContext sourceContext = new FetchSourceContext(true, fields, null);
        searchSourceBuilder.fetchSource(sourceContext);
    }

    @Override
    public Collection<RowData> lookup(RowData keyRow) {

        final String index = indexGenerator.generate(keyRow);
        request.indices(index);
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery();

        for (int i = 0; i < keyNames.length; i++) {
            queryBuilder.must(QueryBuilders.matchPhraseQuery(keyNames[i], ((Row) converter.toExternal(keyRow)).getField(i)));
        }

        searchSourceBuilder.query(queryBuilder);
        request.source(searchSourceBuilder);
        ArrayList<RowData> rows = new ArrayList<>();
        try {
            SearchResponse response = this.client.search(request, RequestOptions.DEFAULT);
            String[] sourceAaStrings = Stream
                    .of(response.getHits().getHits())
                    .map(SearchHit::getSourceAsString)
                    .toArray(String[]::new);

            for (String s: sourceAaStrings) {
                RowData row = format.deserialize(s.getBytes());
                rows.add(row);
            }
            return rows;
        } catch (IOException e) {
            e.printStackTrace();
        }

        return Collections.emptyList();
    }

    @Override
    public void close() throws Exception {
        super.close();
    }

    private static RestClientBuilder configureRestClientBuilder(
            RestClientBuilder builder, NetworkClientConfig networkClientConfig) {
        if (networkClientConfig.getConnectionPathPrefix() != null) {
            builder.setPathPrefix(networkClientConfig.getConnectionPathPrefix());
        }
        if (networkClientConfig.getPassword() != null
                && networkClientConfig.getUsername() != null) {
            final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
            credentialsProvider.setCredentials(
                    AuthScope.ANY,
                    new UsernamePasswordCredentials(
                            networkClientConfig.getUsername(), networkClientConfig.getPassword()));
            builder.setHttpClientConfigCallback(
                    httpClientBuilder ->
                            httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider));
        }
        if (networkClientConfig.getConnectionRequestTimeout() != null
                || networkClientConfig.getConnectionTimeout() != null
                || networkClientConfig.getSocketTimeout() != null) {
            builder.setRequestConfigCallback(
                    requestConfigBuilder -> {
                        if (networkClientConfig.getConnectionRequestTimeout() != null) {
                            requestConfigBuilder.setConnectionRequestTimeout(
                                    networkClientConfig.getConnectionRequestTimeout());
                        }
                        if (networkClientConfig.getConnectionTimeout() != null) {
                            requestConfigBuilder.setConnectTimeout(
                                    networkClientConfig.getConnectionTimeout());
                        }
                        if (networkClientConfig.getSocketTimeout() != null) {
                            requestConfigBuilder.setSocketTimeout(
                                    networkClientConfig.getSocketTimeout());
                        }
                        return requestConfigBuilder;
                    });
        }
        return builder;
    }

}
