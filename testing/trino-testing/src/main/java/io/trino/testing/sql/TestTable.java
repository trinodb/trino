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
package io.trino.testing.sql;

import com.google.common.collect.ImmutableList;

import java.security.SecureRandom;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.lang.Character.MAX_RADIX;
import static java.lang.Math.abs;
import static java.lang.Math.min;
import static java.lang.String.format;
import static java.lang.String.join;

public class TestTable
        implements AutoCloseable
{
    private static final SecureRandom random = new SecureRandom();
    // The suffix needs to be long enough to "prevent" collisions in practice. The length of 5 was proven not to be long enough
    private static final int RANDOM_SUFFIX_LENGTH = 10;

    private final SqlExecutor sqlExecutor;
    private final String name;

    public TestTable(SqlExecutor sqlExecutor, String namePrefix, String tableDefinition)
    {
        this(sqlExecutor, namePrefix, tableDefinition, ImmutableList.of());
    }

    public TestTable(SqlExecutor sqlExecutor, String namePrefix, String tableDefinition, List<String> rowsToInsert)
    {
        this.sqlExecutor = sqlExecutor;
        this.name = namePrefix + randomTableSuffix();
        sqlExecutor.execute(format("CREATE TABLE %s %s", name, tableDefinition));
        try {
            for (String row : rowsToInsert) {
                // some databases do not support multi value insert statement
                sqlExecutor.execute(format("INSERT INTO %s VALUES (%s)", name, row));
            }
        }
        catch (Exception e) {
            try (TestTable ignored = this) {
                throw e;
            }
        }
    }

    public String getName()
    {
        return name;
    }

    public static TestTable fromColumns(SqlExecutor sqlExecutor, String namePrefix, Map<String, List<String>> columns)
    {
        return fromColumns(
                sqlExecutor,
                namePrefix,
                columns,
                column -> {
                    throw new IllegalArgumentException(String.format("Some values missing for column '%s'", column));
                });
    }

    public static TestTable fromColumns(SqlExecutor sqlExecutor, String namePrefix, Map<String, List<String>> columns, String defaultValue)
    {
        return fromColumns(sqlExecutor, namePrefix, columns, column -> defaultValue);
    }

    private static TestTable fromColumns(SqlExecutor sqlExecutor, String namePrefix, Map<String, List<String>> columns, Function<String, String> defaultValueSupplier)
    {
        int rowsCount = columns.values().stream()
                .mapToInt(List::size)
                .max().orElseThrow(() -> new IllegalArgumentException("please, give me at least one column to work with"));
        return fromColumnValueProviders(
                sqlExecutor,
                namePrefix,
                rowsCount,
                columns.entrySet().stream()
                        .collect(toImmutableMap(
                                Map.Entry::getKey,
                                entry -> index -> {
                                    if (index < entry.getValue().size()) {
                                        return entry.getValue().get(index);
                                    }
                                    return defaultValueSupplier.apply(entry.getKey());
                                })));
    }

    public static TestTable fromColumnValueProviders(SqlExecutor sqlExecutor, String namePrefix, int rowsCount, Map<String, Function<Integer, String>> columnsValueProviders)
    {
        String tableDefinition = "(" + join(",", columnsValueProviders.keySet()) + ")";

        ImmutableList.Builder<String> rows = ImmutableList.builder();
        for (int rowId = 0; rowId < rowsCount; rowId++) {
            ImmutableList.Builder<String> rowValues = ImmutableList.builder();
            for (Function<Integer, String> columnValues : columnsValueProviders.values()) {
                rowValues.add(columnValues.apply(rowId));
            }
            rows.add(join(",", rowValues.build()));
        }
        return new TestTable(sqlExecutor, namePrefix, tableDefinition, rows.build());
    }

    @Override
    public void close()
    {
        sqlExecutor.execute("DROP TABLE " + name);
    }

    public static String randomTableSuffix()
    {
        String randomSuffix = Long.toString(abs(random.nextLong()), MAX_RADIX);
        return randomSuffix.substring(0, min(RANDOM_SUFFIX_LENGTH, randomSuffix.length()));
    }
}
