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
package io.trino.plugin.hsqldb;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.trino.plugin.jdbc.TablePropertiesProvider;
import io.trino.spi.session.PropertyMetadata;
import io.trino.spi.type.ArrayType;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;

public class HsqlDbTableProperties
        implements TablePropertiesProvider
{
    public static final String PRIMARY_KEY_PROPERTY = "primary_key";
    public static final String TABLE_TYPE_PROPERTY = "table_type";

    private final List<PropertyMetadata<?>> tableProperties;

    @Inject
    public HsqlDbTableProperties()
    {
        tableProperties = ImmutableList.<PropertyMetadata<?>>builder()
                .add(new PropertyMetadata<>(
                        PRIMARY_KEY_PROPERTY,
                        "The primary keys for the table",
                        new ArrayType(VARCHAR),
                        List.class,
                        ImmutableList.of(),
                        false,
                        value -> (List<?>) value,
                        value -> value))

                .add(new PropertyMetadata<>(
                        TABLE_TYPE_PROPERTY,
                        "The table type to create",
                        VARCHAR,
                        String.class,
                        "CACHED",
                        false,
                        value -> (String) value,
                        value -> value))

                .build();
    }

    @Override
    public List<PropertyMetadata<?>> getTableProperties()
    {
        return tableProperties;
    }

    @SuppressWarnings("unchecked")
    public static Optional<List<String>> getPrimaryKey(Map<String, Object> tableProperties)
    {
        requireNonNull(tableProperties, "tableProperties is null");
        return Optional.ofNullable((List<String>) tableProperties.get(PRIMARY_KEY_PROPERTY));
    }

    @SuppressWarnings("unchecked")
    public static Optional<String> getTableType(Map<String, Object> tableProperties)
    {
        requireNonNull(tableProperties, "tableProperties is null");
        return Optional.ofNullable((String) tableProperties.get(TABLE_TYPE_PROPERTY));
    }
}
