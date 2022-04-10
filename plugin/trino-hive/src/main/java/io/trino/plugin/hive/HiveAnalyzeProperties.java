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
package io.trino.plugin.hive;

import com.google.common.collect.ImmutableList;
import io.trino.spi.TrinoException;
import io.trino.spi.session.PropertyMetadata;
import io.trino.spi.type.ArrayType;

import javax.inject.Inject;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.plugin.hive.HivePartitionKey.HIVE_DEFAULT_DYNAMIC_PARTITION;
import static io.trino.spi.StandardErrorCode.INVALID_ANALYZE_PROPERTY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.String.format;

public class HiveAnalyzeProperties
{
    public static final String PARTITIONS_PROPERTY = "PARTITIONS";
    public static final String COLUMNS_PROPERTY = "COLUMNS";

    private final List<PropertyMetadata<?>> analyzeProperties;

    @Inject
    public HiveAnalyzeProperties()
    {
        analyzeProperties = ImmutableList.of(
                new PropertyMetadata<>(
                        PARTITIONS_PROPERTY,
                        "Partitions to be analyzed",
                        new ArrayType(new ArrayType(VARCHAR)),
                        List.class,
                        null,
                        false,
                        HiveAnalyzeProperties::decodePartitionLists,
                        value -> value),
                new PropertyMetadata<>(
                        COLUMNS_PROPERTY,
                        "Columns to be analyzed",
                        new ArrayType(VARCHAR),
                        Set.class,
                        null,
                        false,
                        HiveAnalyzeProperties::decodeColumnNames,
                        value -> value));
    }

    public List<PropertyMetadata<?>> getAnalyzeProperties()
    {
        return analyzeProperties;
    }

    @SuppressWarnings("unchecked")
    public static Optional<List<List<String>>> getPartitionList(Map<String, Object> properties)
    {
        List<List<String>> partitions = (List<List<String>>) properties.get(PARTITIONS_PROPERTY);
        return Optional.ofNullable(partitions);
    }

    private static List<List<String>> decodePartitionLists(Object object)
    {
        if (object == null) {
            return null;
        }

        // replace null partition value with hive default partition
        return ((Collection<?>) object).stream()
                .peek(partition -> throwIfNull(partition, "partitions"))
                .map(partition -> ((Collection<?>) partition).stream()
                        .map(name -> firstNonNull((String) name, HIVE_DEFAULT_DYNAMIC_PARTITION))
                        .collect(toImmutableList()))
                .distinct()
                .collect(toImmutableList());
    }

    public static Optional<Set<String>> getColumnNames(Map<String, Object> properties)
    {
        @SuppressWarnings("unchecked")
        Set<String> columns = (Set<String>) properties.get(COLUMNS_PROPERTY);
        return Optional.ofNullable(columns);
    }

    private static Set<String> decodeColumnNames(Object object)
    {
        if (object == null) {
            return null;
        }

        Collection<?> columns = ((Collection<?>) object);
        return columns.stream()
                .peek(property -> throwIfNull(property, "columns"))
                .map(String.class::cast)
                .collect(toImmutableSet());
    }

    private static void throwIfNull(Object object, String propertyName)
    {
        if (object == null) {
            throw new TrinoException(INVALID_ANALYZE_PROPERTY, format("Invalid null value in analyze %s property", propertyName));
        }
    }
}
