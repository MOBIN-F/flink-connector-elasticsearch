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
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.config.TableConfigOptions;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.Projection;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DeserializationFormatFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.util.StringUtils;

import java.time.ZoneId;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.connector.elasticsearch.table.ElasticsearchConnectorOptions.CONNECTION_REQUEST_TIMEOUT;
import static org.apache.flink.connector.elasticsearch.table.ElasticsearchConnectorOptions.CONNECTION_TIMEOUT;
import static org.apache.flink.connector.elasticsearch.table.ElasticsearchConnectorOptions.FORMAT_OPTION;
import static org.apache.flink.connector.elasticsearch.table.ElasticsearchConnectorOptions.HOSTS_OPTION;
import static org.apache.flink.connector.elasticsearch.table.ElasticsearchConnectorOptions.INDEX_OPTION;
import static org.apache.flink.connector.elasticsearch.table.ElasticsearchConnectorOptions.PASSWORD_OPTION;
import static org.apache.flink.connector.elasticsearch.table.ElasticsearchConnectorOptions.SOCKET_TIMEOUT;
import static org.apache.flink.connector.elasticsearch.table.ElasticsearchConnectorOptions.USERNAME_OPTION;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.elasticsearch.common.Strings.capitalize;

/** A {@link DynamicTableSourceFactory} for discovering ElasticsearchDynamicSource. */
@Internal
public class ElasticsearchDynamicSourceFactoryBase implements DynamicTableSourceFactory {
    private final String factoryIdentifier;

    public ElasticsearchDynamicSourceFactoryBase(
            String factoryIdentifier

    ) {
        this.factoryIdentifier = checkNotNull(factoryIdentifier);
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        List<LogicalTypeWithIndex> primaryKeyLogicalTypesWithIndex =
                getPrimaryKeyLogicalTypesWithIndex(context);

        final FactoryUtil.TableFactoryHelper helper =
                FactoryUtil.createTableFactoryHelper(this, context);
        DecodingFormat<DeserializationSchema<RowData>> format =
                helper.discoverDecodingFormat(DeserializationFormatFactory.class, FORMAT_OPTION);

        ElasticsearchConfiguration config = getConfiguration(helper);
        helper.validate();
        validateConfiguration(config);

        return new ElasticsearchDynamicSource(
                format,
                config,
                primaryKeyLogicalTypesWithIndex,
                context.getPhysicalRowDataType(),
                capitalize(factoryIdentifier),
                getLocalTimeZoneId(context.getConfiguration())
                );
    }

    ZoneId getLocalTimeZoneId(ReadableConfig readableConfig) {
        final String zone = readableConfig.get(TableConfigOptions.LOCAL_TIME_ZONE);
        final ZoneId zoneId =
                TableConfigOptions.LOCAL_TIME_ZONE.defaultValue().equals(zone)
                        ? ZoneId.systemDefault()
                        : ZoneId.of(zone);
        return zoneId;
    }

    void validateConfiguration(ElasticsearchConfiguration config) {
        config.getHosts(); // validate hosts
        validate(
                config.getIndex().length() >= 1,
                () -> String.format("'%s' must not be empty", INDEX_OPTION.key()));
        if (config.getUsername().isPresent()
                && !StringUtils.isNullOrWhitespaceOnly(config.getUsername().get())) {
            validate(
                    config.getPassword().isPresent()
                            && !StringUtils.isNullOrWhitespaceOnly(config.getPassword().get()),
                    () ->
                            String.format(
                                    "'%s' and '%s' must be set at the same time. Got: username '%s' and password '%s'",
                                    USERNAME_OPTION.key(),
                                    PASSWORD_OPTION.key(),
                                    config.getUsername().get(),
                                    config.getPassword().orElse("")));
        }
    }

    static void validate(boolean condition, Supplier<String> message) {
        if (!condition) {
            throw new ValidationException(message.get());
        }
    }

    ElasticsearchConfiguration getConfiguration(FactoryUtil.TableFactoryHelper helper) {
        return new ElasticsearchConfiguration(helper.getOptions());
    }

    List<LogicalTypeWithIndex> getPrimaryKeyLogicalTypesWithIndex(Context context) {
        DataType physicalRowDataType = context.getPhysicalRowDataType();
        int[] primaryKeyIndexes = context.getPrimaryKeyIndexes();
        if (primaryKeyIndexes.length != 0) {
            DataType pkDataType = Projection.of(primaryKeyIndexes).project(physicalRowDataType);

            ElasticsearchValidationUtils.validatePrimaryKey(pkDataType);
        }

        ResolvedSchema resolvedSchema = context.getCatalogTable().getResolvedSchema();
        return Arrays.stream(primaryKeyIndexes)
                .mapToObj(
                        index -> {
                            Optional<Column> column = resolvedSchema.getColumn(index);
                            if (!column.isPresent()) {
                                throw new IllegalStateException(
                                        String.format(
                                                "No primary key column found with index '%s'.",
                                                index));
                            }
                            LogicalType logicalType = column.get().getDataType().getLogicalType();
                            return new LogicalTypeWithIndex(index, logicalType);
                        })
                .collect(Collectors.toList());
    }

    @Override
    public String factoryIdentifier() {
        return factoryIdentifier;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return Stream.of(HOSTS_OPTION, INDEX_OPTION).collect(Collectors.toSet());
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return Stream.of(
                CONNECTION_REQUEST_TIMEOUT,
                CONNECTION_TIMEOUT,
                SOCKET_TIMEOUT,
                FORMAT_OPTION,
                PASSWORD_OPTION,
                USERNAME_OPTION)
                .collect(Collectors.toSet());
    }
}
