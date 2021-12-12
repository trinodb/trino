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
package io.trino.tests.product.hive;

import com.google.common.math.IntMath;
import io.trino.tempto.ProductTest;
import io.trino.tempto.Requirement;
import io.trino.tempto.RequirementsProvider;
import io.trino.tempto.configuration.Configuration;
import io.trino.tempto.fulfillment.table.MutableTableRequirement;
import io.trino.tempto.fulfillment.table.MutableTablesState;
import io.trino.tempto.fulfillment.table.TableDefinition;
import io.trino.tempto.fulfillment.table.hive.HiveDataSource;
import io.trino.tempto.fulfillment.table.hive.HiveTableDefinition;
import io.trino.tempto.query.QueryResult;
import io.trino.testng.services.Flaky;
import org.testng.annotations.Test;

import javax.inject.Inject;

import java.math.RoundingMode;
import java.util.Optional;
import java.util.stream.IntStream;

import static io.trino.tempto.Requirements.compose;
import static io.trino.tempto.assertions.QueryAssert.Row.row;
import static io.trino.tempto.assertions.QueryAssert.assertQueryFailure;
import static io.trino.tempto.assertions.QueryAssert.assertThat;
import static io.trino.tempto.fulfillment.table.TableRequirements.immutableTable;
import static io.trino.tempto.fulfillment.table.TableRequirements.mutableTable;
import static io.trino.tempto.fulfillment.table.hive.InlineDataSource.createResourceDataSource;
import static io.trino.tempto.fulfillment.table.hive.InlineDataSource.createStringDataSource;
import static io.trino.tempto.fulfillment.table.hive.tpch.TpchTableDefinitions.NATION;
import static io.trino.tempto.query.QueryExecutor.query;
import static io.trino.tests.product.TestGroups.HIVE_PARTITIONING;
import static io.trino.tests.product.hive.HiveProductTest.ERROR_COMMITTING_WRITE_TO_HIVE_ISSUE;
import static io.trino.tests.product.hive.HiveProductTest.ERROR_COMMITTING_WRITE_TO_HIVE_MATCH;
import static java.lang.Math.min;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;

public class TestHivePartitionsTable
        extends ProductTest
        implements RequirementsProvider
{
    // We use hive.max-partitions-per-scan=100 in tests, so this many partitions is too many
    private static final int TOO_MANY_PARTITIONS = 105;

    private static final String PARTITIONED_TABLE = "partitioned_table";
    private static final String PARTITIONED_TABLE_WITH_VARIABLE_PARTITIONS = "partitioned_table_with_variable_partitions";

    @Inject
    private MutableTablesState tablesState;

    @Override
    public Requirement getRequirements(Configuration configuration)
    {
        return compose(
                immutableTable(NATION),
                mutableTable(partitionedTableDefinition(), PARTITIONED_TABLE, MutableTableRequirement.State.CREATED),
                mutableTable(partitionedTableWithVariablePartitionsDefinition(configuration.getBoolean("databases.hive.enforce_non_transactional_tables")), PARTITIONED_TABLE_WITH_VARIABLE_PARTITIONS, MutableTableRequirement.State.CREATED));
    }

    private static TableDefinition partitionedTableDefinition()
    {
        String createTableDdl = "CREATE EXTERNAL TABLE %NAME%(col INT) " +
                "PARTITIONED BY (part_col INT) " +
                "STORED AS ORC";

        HiveDataSource dataSource = createResourceDataSource(PARTITIONED_TABLE, "io/trino/tests/product/hive/data/single_int_column/data.orc");
        HiveDataSource invalidData = createStringDataSource(PARTITIONED_TABLE, "INVALID DATA");
        return HiveTableDefinition.builder(PARTITIONED_TABLE)
                .setCreateTableDDLTemplate(createTableDdl)
                .addPartition("part_col = 1", invalidData)
                .addPartition("part_col = 2", dataSource)
                .build();
    }

    private static TableDefinition partitionedTableWithVariablePartitionsDefinition(Optional<Boolean> createTablesAsAcid)
    {
        String createTableDdl = "CREATE TABLE %NAME%(col INT) " +
                "PARTITIONED BY (part_col INT) " +
                "STORED AS ORC " +
                (createTablesAsAcid.orElse(false) ? "TBLPROPERTIES ('transactional_properties' = 'none', 'transactional' = 'false')" : "");

        return HiveTableDefinition.builder(PARTITIONED_TABLE_WITH_VARIABLE_PARTITIONS)
                .setCreateTableDDLTemplate(createTableDdl)
                .setNoData()
                .build();
    }

    @Test(groups = HIVE_PARTITIONING)
    @Flaky(issue = ERROR_COMMITTING_WRITE_TO_HIVE_ISSUE, match = ERROR_COMMITTING_WRITE_TO_HIVE_MATCH)
    public void testShowPartitionsFromHiveTable()
    {
        String tableNameInDatabase = tablesState.get(PARTITIONED_TABLE).getNameInDatabase();
        String partitionsTable = "\"" + tableNameInDatabase + "$partitions\"";

        QueryResult partitionListResult;

        partitionListResult = query("SELECT * FROM " + partitionsTable);
        assertThat(partitionListResult).containsExactlyInOrder(row(1), row(2));
        assertColumnNames(partitionListResult, "part_col");

        partitionListResult = query(format("SELECT * FROM %s WHERE part_col = 1", partitionsTable));
        assertThat(partitionListResult).containsExactlyInOrder(row(1));
        assertColumnNames(partitionListResult, "part_col");

        assertQueryFailure(() -> query(format("SELECT * FROM %s WHERE no_such_column = 1", partitionsTable)))
                .hasMessageContaining("Column 'no_such_column' cannot be resolved");
        assertQueryFailure(() -> query(format("SELECT * FROM %s WHERE col = 1", partitionsTable)))
                .hasMessageContaining("Column 'col' cannot be resolved");
    }

    @Test(groups = HIVE_PARTITIONING)
    @Flaky(issue = ERROR_COMMITTING_WRITE_TO_HIVE_ISSUE, match = ERROR_COMMITTING_WRITE_TO_HIVE_MATCH)
    public void testShowPartitionsFromUnpartitionedTable()
    {
        assertQueryFailure(() -> query("SELECT * FROM \"nation$partitions\""))
                .hasMessageMatching(".*Table 'hive.default.nation\\$partitions' does not exist");
    }

    @Test(groups = HIVE_PARTITIONING)
    @Flaky(issue = ERROR_COMMITTING_WRITE_TO_HIVE_ISSUE, match = ERROR_COMMITTING_WRITE_TO_HIVE_MATCH)
    public void testShowPartitionsFromHiveTableWithTooManyPartitions()
    {
        String tableName = tablesState.get(PARTITIONED_TABLE_WITH_VARIABLE_PARTITIONS).getNameInDatabase();
        String partitionsTable = "\"" + tableName + "$partitions\"";
        createPartitions(tableName, TOO_MANY_PARTITIONS);

        // Verify we created enough partitions for the test to be meaningful
        assertThatThrownBy(() -> query("SELECT * FROM " + tableName))
                .hasMessageMatching(".*: Query over table '\\S+' can potentially read more than \\d+ partitions");

        QueryResult partitionListResult;

        partitionListResult = query(format("SELECT * FROM %s WHERE part_col < 7", partitionsTable));
        assertThat(partitionListResult).containsExactlyInOrder(row(0), row(1), row(2), row(3), row(4), row(5), row(6));
        assertColumnNames(partitionListResult, "part_col");

        partitionListResult = query(format("SELECT a.part_col FROM (SELECT * FROM %s WHERE part_col = 1) a, (SELECT * FROM %s WHERE part_col = 1) b WHERE a.col = b.col", tableName, tableName));
        assertThat(partitionListResult).containsExactlyInOrder(row(1));

        partitionListResult = query(format("SELECT * FROM %s WHERE part_col < -10", partitionsTable));
        assertThat(partitionListResult).hasNoRows();

        partitionListResult = query(format("SELECT * FROM %s ORDER BY part_col LIMIT 7", partitionsTable));
        assertThat(partitionListResult).containsExactlyInOrder(row(0), row(1), row(2), row(3), row(4), row(5), row(6));
    }

    private void createPartitions(String tableName, int partitionsToCreate)
    {
        // This is done in tests rather than as a requirement, because TableRequirements.immutableTable cannot be partitioned
        // and mutable table is recreated before every test (and this takes a lot of time).
        // TODO convert to immutableTable + TableDefinition once immutableTable supports partitioned tables

        requireNonNull(tableName, "tableName is null");

        int maxPartitionsAtOnce = 100;

        IntStream.range(0, IntMath.divide(partitionsToCreate, maxPartitionsAtOnce, RoundingMode.UP))
                .forEach(batch -> {
                    int rangeStart = batch * maxPartitionsAtOnce;
                    int rangeEndInclusive = min((batch + 1) * maxPartitionsAtOnce, partitionsToCreate) - 1;
                    query(format(
                            "INSERT INTO %s (part_col, col) " +
                                    "SELECT CAST(id AS integer), 42 FROM UNNEST (sequence(%s, %s)) AS u(id)",
                            tableName,
                            rangeStart,
                            rangeEndInclusive));
                });
    }

    private static void assertColumnNames(QueryResult queryResult, String... columnNames)
    {
        for (int i = 0; i < columnNames.length; i++) {
            assertEquals(queryResult.tryFindColumnIndex(columnNames[i]), Optional.of(i + 1), "Index of column " + columnNames[i]);
        }
        assertEquals(queryResult.getColumnsCount(), columnNames.length);
    }
}
