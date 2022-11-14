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
package io.trino.plugin.cassandra;

import com.google.common.collect.ImmutableList;
import io.trino.testing.sql.SqlExecutor;

import java.util.List;
import java.util.function.Function;

import static com.google.common.base.Verify.verify;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.lang.String.format;
import static java.lang.String.join;
import static java.util.stream.Collectors.joining;

public class TestCassandraTable
        implements AutoCloseable
{
    private final SqlExecutor sqlExecutor;
    private final String keyspace;
    private final String tableName;

    // Make sure the `namePrefix` in the following constructors does not exceed 38 characters,
    // because Cassandra table names are limited to 48 characters in size, so we can only use
    // a maximum of 38 characters without the suffix.
    // https://cassandra.apache.org/doc/trunk/cassandra/cql/ddl.html#common-definitions
    //
    // NOTE: "38 characters" may be changed in the future, please refer to
    // `48 - RANDOM_SUFFIX_LENGTH` for the exact value.

    public TestCassandraTable(
            SqlExecutor sqlExecutor,
            CassandraServer server,
            String keyspace,
            String namePrefix,
            List<ColumnDefinition> columnDefinitions,
            List<String> rowsToInsert)
    {
        this.sqlExecutor = sqlExecutor;
        this.keyspace = keyspace;
        this.tableName = namePrefix + randomNameSuffix();
        sqlExecutor.execute(format("CREATE TABLE %s.%s %s", keyspace, tableName, tableDefinition(columnDefinitions)));
        String columns = columnDefinitions.stream()
                .map(columnDefinition -> columnDefinition.name)
                .collect(joining(", "));
        try {
            for (String row : rowsToInsert) {
                // Cassandra does not support multi value insert statement
                sqlExecutor.execute(format("INSERT INTO %s.%s (%s) VALUES (%s)", keyspace, tableName, columns, row));
            }

            // Ensure that the currently created table is visible to other sessions (e.g. the session used in DistributedQueryRunner)
            server.refreshSizeEstimates(keyspace, tableName);
        }
        catch (Exception e) {
            try (TestCassandraTable ignored = this) {
                throw new RuntimeException(e);
            }
        }
    }

    public String getTableName()
    {
        return format("%s.%s", keyspace, tableName);
    }

    private static String tableDefinition(List<ColumnDefinition> columnDefinitions)
    {
        ImmutableList.Builder<String> partitionColumns = ImmutableList.builder();
        ImmutableList.Builder<String> clusterColumns = ImmutableList.builder();
        for (ColumnDefinition columnDefinition : columnDefinitions) {
            if (columnDefinition.primaryKeyType == PrimaryKeyType.PARTITION) {
                partitionColumns.add(columnDefinition.name);
            }
            else if (columnDefinition.primaryKeyType == PrimaryKeyType.CLUSTER) {
                clusterColumns.add(columnDefinition.name);
            }
        }
        List<String> partitions = partitionColumns.build();
        List<String> clusters = clusterColumns.build();

        verify(partitions.size() > 0, "Cassandra table must have at least one partition key");

        String primaryKey = format(
                "(%s%s)",
                partitions.stream().collect(joining(", ", "(", ")")),
                clusters.isEmpty() ? "" : clusters.stream().collect(joining(", ", ", ", "")));

        return columnDefinitions.stream()
                .map(columnDefinition -> columnDefinition.name + " " + columnDefinition.type)
                .collect(joining(",", "(", ",PRIMARY KEY " + primaryKey + ")"));
    }

    @Override
    public void close()
    {
        sqlExecutor.execute("DROP TABLE " + getTableName());
    }

    public static ColumnDefinition partitionColumn(String name, String type)
    {
        return new ColumnDefinition(name, type, PrimaryKeyType.PARTITION);
    }

    public static ColumnDefinition clusterColumn(String name, String type)
    {
        return new ColumnDefinition(name, type, PrimaryKeyType.CLUSTER);
    }

    public static ColumnDefinition generalColumn(String name, String type)
    {
        return new ColumnDefinition(name, type, PrimaryKeyType.GENERAL);
    }

    public static List<String> columnsValue(int rowsCount, List<Function<Integer, String>> columnsValueProviders)
    {
        ImmutableList.Builder<String> rows = ImmutableList.builder();
        for (int rowNumber = 1; rowNumber <= rowsCount; rowNumber++) {
            ImmutableList.Builder<String> rowValues = ImmutableList.builder();
            for (Function<Integer, String> columnValues : columnsValueProviders) {
                rowValues.add(columnValues.apply(rowNumber));
            }
            rows.add(join(",", rowValues.build()));
        }
        return rows.build();
    }

    public static class ColumnDefinition
    {
        private final String name;
        private final String type;
        private final PrimaryKeyType primaryKeyType;

        private ColumnDefinition(String name, String type, PrimaryKeyType primaryKeyType)
        {
            this.name = name;
            this.type = type;
            this.primaryKeyType = primaryKeyType;
        }
    }

    private enum PrimaryKeyType
    {
        PARTITION, CLUSTER, GENERAL;
    }
}
