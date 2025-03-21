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
package io.trino.plugin.databend;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.trino.plugin.jdbc.TablePropertiesProvider;
import io.trino.spi.session.PropertyMetadata;
import io.trino.spi.type.ArrayType;

import java.util.List;
import java.util.Map;

import static io.trino.spi.session.PropertyMetadata.enumProperty;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;

public final class DatabendTableProperties
        implements TablePropertiesProvider
{
    public static final String ENGINE_PROPERTY = "engine";
    public static final String ORDER_BY_PROPERTY = "order_by"; //required

    private final List<PropertyMetadata<?>> tableProperties;

    @Inject
    public DatabendTableProperties()
    {
        tableProperties = ImmutableList.<PropertyMetadata<?>>builder()
                .add(enumProperty(
                        ENGINE_PROPERTY,
                        "Databend Table Engine, defaults to Log",
                        DatabendEngineType.class,
                        DatabendEngineType.FUSE, //  FUSE is default engine of Databend which optimized for both read and write operations with improved indexing and compression.
                        false))
                .add(new PropertyMetadata<>(
                        ORDER_BY_PROPERTY,
                        "columns to be the sorting key, it's required for table MergeTree engine family",
                        new ArrayType(VARCHAR),
                        List.class,
                        ImmutableList.of(),
                        false,
                        value -> (List<?>) value,
                        value -> value))
                .build();
    }

    public static DatabendEngineType getEngine(Map<String, Object> tableProperties)
    {
        requireNonNull(tableProperties, "tableProperties is null");
        return (DatabendEngineType) tableProperties.get(ENGINE_PROPERTY);
    }

    public static List<String> getOrderBy(Map<String, Object> tableProperties)
    {
        requireNonNull(tableProperties, "tableProperties is null");
        @SuppressWarnings("unchecked")
        List<String> orderBy = (List<String>) tableProperties.get("order_by");
        return orderBy;
    }

    @Override
    public List<PropertyMetadata<?>> getTableProperties()
    {
        return tableProperties;
    }
}
