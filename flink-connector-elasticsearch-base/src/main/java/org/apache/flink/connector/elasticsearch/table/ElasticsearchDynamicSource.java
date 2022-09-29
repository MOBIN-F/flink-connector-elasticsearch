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

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.connector.elasticsearch.sink.NetworkClientConfig;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.abilities.SupportsLimitPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsProjectionPushDown;
import org.apache.flink.table.connector.source.lookup.LookupFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.StringUtils;

import org.apache.flink.shaded.guava30.com.google.common.base.Preconditions;

import java.time.ZoneId;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** A {@link ElasticsearchDynamicSource} for ElasticSearch. */
public class ElasticsearchDynamicSource implements ScanTableSource,
        LookupTableSource,
        SupportsProjectionPushDown,
        SupportsLimitPushDown {
    final DecodingFormat<DeserializationSchema<RowData>> format;
    final DataType physicalRowDataType;
    final List<LogicalTypeWithIndex> primaryKeyLogicalTypesWithIndex;
    final ElasticsearchConfiguration config;
    final ZoneId localTimeZoneId;
    final String summaryString;

    public ElasticsearchDynamicSource(
            DecodingFormat<DeserializationSchema<RowData>> format,
            ElasticsearchConfiguration config,
            List<LogicalTypeWithIndex> primaryKeyLogicalTypesWithIndex,
            DataType physicalRowDataType,
            String summaryString,
            ZoneId localTimeZoneId
    ) {
        this.format = checkNotNull(format);
        this.physicalRowDataType = checkNotNull(physicalRowDataType);
        this.primaryKeyLogicalTypesWithIndex = checkNotNull(primaryKeyLogicalTypesWithIndex);
        this.config = checkNotNull(config);
        this.summaryString = summaryString;
        this.localTimeZoneId = localTimeZoneId;
    }

    @Override
    public DynamicTableSource copy() {
        return new ElasticsearchDynamicSource(format, config, primaryKeyLogicalTypesWithIndex, physicalRowDataType, summaryString, localTimeZoneId);
    }

    @Override
    public String asSummaryString() {
        return summaryString;
    }

    @Override
    public LookupRuntimeProvider getLookupRuntimeProvider(LookupContext lookupContext) {
        String userName = null;
        String password = null;
        String connectionPathPrefix = null;
        Integer connectionRequestTimeout = null;
        Integer connectionTimeout = null;
        Integer socketTimeout = null;

        String[] keyNames = new String[lookupContext.getKeys().length];
        DataType[] keyDataType = new DataType[lookupContext.getKeys().length];
        for (int i = 0; i < keyNames.length; i++) {
            int[] innerKeyArr = lookupContext.getKeys()[i];
            Preconditions.checkArgument(
                    innerKeyArr.length == 1, "elasticsearch only support non-nested look up keys");
            keyNames[i] = DataType.getFieldNames(physicalRowDataType).get(innerKeyArr[0]);
            keyDataType[i] = DataType.getFieldDataTypes(physicalRowDataType).get(innerKeyArr[0]);
        }
        final RowType rowType = (RowType) physicalRowDataType.getLogicalType();
        final RowType keyRowType = RowType.of(
                Arrays.stream(keyDataType)
                        .map(DataType::getLogicalType)
                        .toArray(LogicalType[]::new));

        if (config.getUsername().isPresent()
                && !StringUtils.isNullOrWhitespaceOnly(config.getUsername().get())) {
            userName = config.getUsername().get();
        }

        if (config.getPassword().isPresent()
                && !StringUtils.isNullOrWhitespaceOnly(config.getPassword().get())) {
            password = config.getPassword().get();
        }

        if (config.getPathPrefix().isPresent()
                && !StringUtils.isNullOrWhitespaceOnly(config.getPathPrefix().get())) {
            connectionPathPrefix = config.getPathPrefix().get();
        }

        if (config.getConnectionRequestTimeout().isPresent()) {
            connectionTimeout =
                    (int) config.getConnectionRequestTimeout().get().getSeconds();
        }

        if (config.getConnectionTimeout().isPresent()) {
            connectionRequestTimeout = (int) config.getConnectionTimeout().get().getSeconds();
        }

        if (config.getSocketTimeout().isPresent()) {
            socketTimeout = (int) config.getSocketTimeout().get().getSeconds();
        }

        ElasticsearchLookupFunction elasticsearchLookupFunction = new ElasticsearchLookupFunction(
                config.getHosts(),
                new NetworkClientConfig(
                        userName,
                        password,
                        connectionPathPrefix,
                        connectionRequestTimeout,
                        connectionTimeout,
                        socketTimeout
                ),
                createIndexGenerator(),
                createKeyExtractor(),
                keyNames,
                rowType,
                keyRowType,
                format.createRuntimeDecoder(lookupContext, physicalRowDataType)
        );
        return LookupFunctionProvider.of(elasticsearchLookupFunction);
    }

    IndexGenerator createIndexGenerator() {
        return IndexGeneratorFactory.createIndexGenerator(
                config.getIndex(),
                DataType.getFieldNames(physicalRowDataType),
                DataType.getFieldDataTypes(physicalRowDataType),
                localTimeZoneId);
    }

    Function<RowData, String> createKeyExtractor() {
        return KeyExtractor.createKeyExtractor(primaryKeyLogicalTypesWithIndex,
                config.getKeyDelimiter());
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.insertOnly();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext scanContext) {
        return null;
    }

    @Override
    public void applyLimit(long l) {

    }

    @Override
    public boolean supportsNestedProjection() {
        return false;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ElasticsearchDynamicSource that = (ElasticsearchDynamicSource) o;
        return Objects.equals(format, that.format)
                && Objects.equals(physicalRowDataType, that.physicalRowDataType)
                && Objects.equals(
                primaryKeyLogicalTypesWithIndex, that.primaryKeyLogicalTypesWithIndex)
                && Objects.equals(config, that.config)
                && Objects.equals(localTimeZoneId, that.localTimeZoneId)
                && Objects.equals(summaryString, that.summaryString);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                format,
                physicalRowDataType,
                primaryKeyLogicalTypesWithIndex,
                config,
                localTimeZoneId,
                summaryString
                );
    }
}
