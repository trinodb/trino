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
package io.trino.plugin.mysql;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.trino.plugin.jdbc.TablePropertiesProvider;
import io.trino.spi.session.PropertyMetadata;
import io.trino.spi.type.ArrayType;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.spi.session.PropertyMetadata.enumProperty;
import static io.trino.spi.session.PropertyMetadata.integerProperty;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;

/**
 * Class contains all table properties for the Clickhouse connector. Used when creating a table:
 * <p>
 * <pre>CREATE TABLE foo (a VARCHAR , b INT) WITH (engine='Log');</pre>
 * </p>
 */
public final class DorisTableProperties
        implements TablePropertiesProvider
{
    public static final String MODEL_TYPE_PROPERTY = "model_type";
    // MergeTree engine properties
    public static final String KEY_BY_PROPERTY = "key_by"; //required
    public static final String DISTRIBUTED_BY_PROPERTY = "distributed_by"; //optional
    public static final String BUCKETS_PROPERTY = "buckets"; //optional

    public static final DorisEngineType DEFAULT_TABLE_MODEL = DorisEngineType.DUPLICATE;

    private final List<PropertyMetadata<?>> tableProperties;

    @Inject
    public DorisTableProperties()
    {
        tableProperties = ImmutableList.of(
                enumProperty(
                        MODEL_TYPE_PROPERTY,
                        "Doris table model, default DUPLICATE",
                        DorisEngineType.class,
                        null,
                        false),
                new PropertyMetadata<>(
                        KEY_BY_PROPERTY,
                        "model columns",
                        new ArrayType(VARCHAR),
                        List.class,
                        ImmutableList.of(),
                        false,
                        value -> (List<?>) value,
                        value -> value),
                new PropertyMetadata<>(
                        DISTRIBUTED_BY_PROPERTY,
                        "DISTRIBUTED columns",
                        new ArrayType(VARCHAR),
                        List.class,
                        ImmutableList.of(),
                        false,
                        value -> (List<?>) value,
                        value -> value),
                integerProperty(
                        BUCKETS_PROPERTY,
                        "BUCKETS num",
                        1,
                        false));
    }

    public static DorisEngineType getModelType(Map<String, Object> tableProperties)
    {
        requireNonNull(tableProperties, "tableProperties is null");
        return (DorisEngineType) tableProperties.get(MODEL_TYPE_PROPERTY);
    }

    @SuppressWarnings("unchecked")
    public static List<String> getKeyBy(Map<String, Object> tableProperties)
    {
        requireNonNull(tableProperties, "tableProperties is null");
        return (List<String>) tableProperties.get(KEY_BY_PROPERTY);
    }

    @SuppressWarnings("unchecked")
    public static List<String> getDistributedBy(Map<String, Object> tableProperties)
    {
        requireNonNull(tableProperties, "tableProperties is null");
        return (List<String>) tableProperties.get(DISTRIBUTED_BY_PROPERTY);
    }

    public static Optional<Integer> getBuckets(Map<String, Object> tableProperties)
    {
        requireNonNull(tableProperties, "tableProperties is null");

        return Optional.ofNullable(tableProperties.get(BUCKETS_PROPERTY)).map(Integer.class::cast);
    }

    @Override
    public List<PropertyMetadata<?>> getTableProperties()
    {
        return tableProperties;
    }
}
