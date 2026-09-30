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
package io.trino.plugin.ydb;

import io.trino.plugin.jdbc.TablePropertiesProvider;
import io.trino.spi.session.PropertyMetadata;
import io.trino.spi.type.ArrayType;

import java.util.List;
import java.util.Map;

import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;

public class YdbTableProperties
        implements TablePropertiesProvider
{
    public static final String PRIMARY_KEY_PROPERTY = "primary_key";

    private final List<PropertyMetadata<?>> tableProperties = List.of(
            new PropertyMetadata<>(
                    PRIMARY_KEY_PROPERTY,
                    "Columns forming the table primary key, in order",
                    new ArrayType(VARCHAR),
                    List.class,
                    List.of(),
                    false,
                    value -> (List<?>) value,
                    value -> value));

    @Override
    public List<PropertyMetadata<?>> getTableProperties()
    {
        return tableProperties;
    }

    public static List<String> getPrimaryKey(Map<String, Object> tableProperties)
    {
        requireNonNull(tableProperties, "tableProperties is null");
        return ((List<?>) tableProperties.getOrDefault(PRIMARY_KEY_PROPERTY, List.of())).stream()
                .map(String.class::cast)
                .toList();
    }
}
