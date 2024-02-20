/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.clickhouse;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.trino.plugin.jdbc.TablePropertiesProvider;
import io.trino.spi.session.PropertyMetadata;
import io.trino.spi.type.ArrayType;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.spi.session.PropertyMetadata.enumProperty;
import static io.trino.spi.session.PropertyMetadata.stringProperty;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;

/**
 * Class contains all table properties for the Clickhouse connector. Used when creating a table:
 * <p>
 * <pre>CREATE TABLE foo (a VARCHAR , b INT) WITH (engine='Log');</pre>
 * </p>
 */
public final class ClickHouseTableProperties
        implements TablePropertiesProvider
{
    public static final String ENGINE_PROPERTY = "engine";
    // MergeTree engine properties
    public static final String ORDER_BY_PROPERTY = "order_by"; //required
    public static final String PARTITION_BY_PROPERTY = "partition_by"; //optional
    public static final String PRIMARY_KEY_PROPERTY = "primary_key"; //optional
    public static final String SAMPLE_BY_PROPERTY = "sample_by"; //optional

    public static final ClickHouseEngineType DEFAULT_TABLE_ENGINE = ClickHouseEngineType.LOG;

    private final List<PropertyMetadata<?>> tableProperties;

    @Inject
    public ClickHouseTableProperties()
    {
        tableProperties = ImmutableList.of(
                enumProperty(
                        ENGINE_PROPERTY,
                        "ClickHouse Table Engine, defaults to Log",
                        ClickHouseEngineType.class,
                        DEFAULT_TABLE_ENGINE,
                        false),
                new PropertyMetadata<>(
                        ORDER_BY_PROPERTY,
                        "columns to be the sorting key, it's required for table MergeTree engine family",
                        new ArrayType(VARCHAR),
                        List.class,
                        ImmutableList.of(),
                        false,
                        value -> (List<?>) value,
                        value -> value),
                new PropertyMetadata<>(
                        PARTITION_BY_PROPERTY,
                        "columns to be the partition key. it's optional for table MergeTree engine family",
                        new ArrayType(VARCHAR),
                        List.class,
                        ImmutableList.of(),
                        false,
                        value -> (List<?>) value,
                        value -> value),
                new PropertyMetadata<>(
                        PRIMARY_KEY_PROPERTY,
                        "columns to be the primary key. it's optional for table MergeTree engine family",
                        new ArrayType(VARCHAR),
                        List.class,
                        ImmutableList.of(),
                        false,
                        value -> (List<?>) value,
                        value -> value),
                stringProperty(
                        SAMPLE_BY_PROPERTY,
                        "An expression for sampling. it's optional for table MergeTree engine family",
                        null,
                        false));
    }

    public static ClickHouseEngineType getEngine(Map<String, Object> tableProperties)
    {
        requireNonNull(tableProperties, "tableProperties is null");
        return (ClickHouseEngineType) tableProperties.get(ENGINE_PROPERTY);
    }

    @SuppressWarnings("unchecked")
    public static List<String> getOrderBy(Map<String, Object> tableProperties)
    {
        requireNonNull(tableProperties, "tableProperties is null");
        return (List<String>) tableProperties.get(ORDER_BY_PROPERTY);
    }

    @SuppressWarnings("unchecked")
    public static List<String> getPartitionBy(Map<String, Object> tableProperties)
    {
        requireNonNull(tableProperties, "tableProperties is null");
        return (List<String>) tableProperties.get(PARTITION_BY_PROPERTY);
    }

    @SuppressWarnings("unchecked")
    public static List<String> getPrimaryKey(Map<String, Object> tableProperties)
    {
        requireNonNull(tableProperties, "tableProperties is null");
        return (List<String>) tableProperties.get(PRIMARY_KEY_PROPERTY);
    }

    public static Optional<String> getSampleBy(Map<String, Object> tableProperties)
    {
        requireNonNull(tableProperties, "tableProperties is null");

        return Optional.ofNullable(tableProperties.get(SAMPLE_BY_PROPERTY)).map(String.class::cast);
    }

    @Override
    public List<PropertyMetadata<?>> getTableProperties()
    {
        return tableProperties;
    }
}
