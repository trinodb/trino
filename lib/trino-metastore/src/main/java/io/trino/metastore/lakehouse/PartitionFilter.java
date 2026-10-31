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
package io.trino.metastore.lakehouse;

import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slice;
import io.trino.metastore.Column;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.CharType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.metastore.Partitions.HIVE_DEFAULT_DYNAMIC_PARTITION;

/**
 * Builds the partition filter that Lakehouse applies while it lists partitions.
 *
 * <p>Pushing a predicate down is optional: an empty filter is always a safe answer, because Trino
 * applies the full predicate to whatever comes back. Only single values of string types are pushed
 * down, which is what the Glue implementation does by default, because the textual form of every
 * other type has to match how the partition was written.
 */
public final class PartitionFilter
{
    private static final String STRING_TYPE = "string";

    /**
     * How Lakehouse compares a value of a given Hive type.
     */
    private static final Map<String, String> TEMPLATES = ImmutableMap.<String, String>builder()
            .put("tinyint", "%s=%s")
            .put("smallint", "%s=%s")
            .put("int", "%s=%s")
            .put("integer", "%s=%s")
            .put("bigint", "%s=%s")
            .put("date", "%s=CAST('%s' as date)")
            .put("timestamp", "%s=CAST('%s' as timestamp)")
            .put(STRING_TYPE, "%s='%s'")
            .buildOrThrow();

    private PartitionFilter() {}

    /**
     * Returns the filter for a Trino predicate, or an empty string when nothing can be pushed down.
     */
    public static String toLakehouseFilter(List<String> columnNames, TupleDomain<String> partitionKeysFilter)
    {
        Map<String, Domain> domains = partitionKeysFilter.getDomains()
                .orElseThrow(() -> new IllegalArgumentException("partition filter matches nothing"));

        List<String> conditions = new ArrayList<>();
        for (String columnName : columnNames) {
            pushedDownValue(domains.get(columnName))
                    .ifPresent(value -> conditions.add(equality(columnName, STRING_TYPE, value)));
        }
        return String.join(" and ", conditions);
    }

    /**
     * Returns the filter for one partition. A value that cannot be written into the filter stays out
     * of it, so the filter may match more than one partition, and the caller has to pick the exact one.
     * Use {@link #matchesExactly} to tell whether that is the case.
     */
    public static String toLakehouseFilter(List<Column> partitionColumns, List<String> values)
    {
        if (partitionColumns.size() != values.size()) {
            throw new IllegalArgumentException("partition value count must match partition column count");
        }

        List<String> conditions = new ArrayList<>();
        for (int i = 0; i < values.size(); i++) {
            if (!canBeWrittenInFilter(values.get(i))) {
                continue;
            }
            Column column = partitionColumns.get(i);
            conditions.add(equality(column.getName(), column.getType().toString(), values.get(i)));
        }
        return String.join(" and ", conditions);
    }

    /**
     * Returns whether the filter for these values matches the partition alone, so that no value was
     * left out of it.
     */
    public static boolean matchesExactly(List<String> values)
    {
        return values.stream().allMatch(PartitionFilter::canBeWrittenInFilter);
    }

    // A quote ends the literal and changes the filter
    private static boolean canBeWrittenInFilter(String value)
    {
        return value.indexOf('\'') < 0;
    }

    private static String equality(String name, String hiveType, String value)
    {
        if (HIVE_DEFAULT_DYNAMIC_PARTITION.equals(value)) {
            return name + " is null";
        }
        return TEMPLATES.getOrDefault(hiveType, "%s='%s'").formatted(name, value);
    }

    private static Optional<String> pushedDownValue(Domain domain)
    {
        if (domain == null || !domain.isSingleValue()) {
            return Optional.empty();
        }
        Type type = domain.getType();
        if (!(type instanceof VarcharType) && !(type instanceof CharType)) {
            return Optional.empty();
        }
        if (!(domain.getSingleValue() instanceof Slice slice)) {
            return Optional.empty();
        }
        String value = slice.toStringUtf8();
        // Such a value stays for Trino to apply
        return canBeWrittenInFilter(value) ? Optional.of(value) : Optional.empty();
    }
}
