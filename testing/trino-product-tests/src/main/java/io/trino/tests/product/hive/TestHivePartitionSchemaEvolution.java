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

import com.google.inject.name.Named;
import io.airlift.log.Logger;
import io.trino.tempto.BeforeTestWithContext;
import io.trino.tempto.assertions.QueryAssert;
import io.trino.tempto.query.QueryExecutionException;
import io.trino.tests.product.hive.util.TemporaryHiveTable;
import org.testng.annotations.Test;

import javax.inject.Inject;

import java.sql.SQLException;
import java.util.function.Supplier;

import static io.trino.tempto.assertions.QueryAssert.Row.row;
import static io.trino.tempto.assertions.QueryAssert.assertThat;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.tests.product.hive.util.TemporaryHiveTable.temporaryHiveTable;
import static io.trino.tests.product.utils.JdbcDriverUtils.setSessionProperty;
import static io.trino.tests.product.utils.QueryExecutors.onHive;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;
import static java.lang.String.format;

public class TestHivePartitionSchemaEvolution
        extends HiveProductTest
{
    private static final Logger log = Logger.get(TestHivePartitionSchemaEvolution.class);

    @Inject
    @Named("databases.hive.enforce_non_transactional_tables")
    private boolean createTablesAsAcid;

    @BeforeTestWithContext
    public void useColumnMappingByName()
            throws SQLException
    {
        setSessionProperty(onTrino().getConnection(), "hive.parquet_use_column_names", "true");
        setSessionProperty(onTrino().getConnection(), "hive.orc_use_column_names", "true");
    }

    @Test
    public void testParquet()
    {
        test(() -> createTable("PARQUET"));
    }

    @Test
    public void testOrc()
    {
        test(() -> createTable("ORC"));
    }

    private void test(Supplier<TemporaryHiveTable> temporaryHiveTableSupplier)
    {
        try (TemporaryHiveTable table = temporaryHiveTableSupplier.get()) {
            // dropping column on table, simulates creating a column on partition
            // adding column on table, simulates dropping a column on partition

            // partition is adding a column at the start
            testEvolution(table, "ALTER TABLE %s REPLACE COLUMNS (float_column float, varchar_column varchar(20))", row(1.1, "jeden", 1));

            // partition is adding a column in the middle
            testEvolution(table, "ALTER TABLE %s REPLACE COLUMNS (int_column int, varchar_column varchar(20))", row(1, "jeden", 1));

            // partition is adding a column at the end
            testEvolution(table, "ALTER TABLE %s REPLACE COLUMNS (int_column int, float_column float)", row(1, 1.1, 1));

            // partition is dropping a column at the start
            testEvolution(table, "ALTER TABLE %s REPLACE COLUMNS (tiny_column tinyint, int_column int, float_column float, varchar_column varchar(20))", row(null, 1, 1.1, "jeden", 1));

            // partition is dropping a column in the middle
            testEvolution(table, "ALTER TABLE %s REPLACE COLUMNS (int_column int, tiny_column tinyint, float_column float, varchar_column varchar(20))", row(1, null, 1.1, "jeden", 1));

            // partition is dropping a column at the end
            testEvolution(table, "ALTER TABLE %s REPLACE COLUMNS (int_column int, float_column float, varchar_column varchar(20), tiny_column tinyint)", row(1, 1.1, "jeden", null, 1));

            // partition is dropping and adding column in the middle
            testEvolution(table, "ALTER TABLE %s REPLACE COLUMNS (int_column int, tiny_column tinyint, varchar_column varchar(20))", row(1, null, "jeden", 1));

            // partition is adding coercions
            testEvolution(table, "ALTER TABLE %s REPLACE COLUMNS (int_column bigint, float_column double, varchar_column varchar(20))", row(1, 1.1, "jeden", 1));

            // partition is swapping columns with coercions
            testEvolution(table, "ALTER TABLE %s REPLACE COLUMNS (varchar_column varchar(20), float_column double, int_column bigint)", row("jeden", 1.1, 1, 1));

            // partition is swapping columns and partition with coercions and is adding a column
            testEvolution(table, "ALTER TABLE %s REPLACE COLUMNS (float_column double, int_column bigint)", row(1.1, 1, 1));

            // partition is swapping columns and partition with coercions and is removing a column
            testEvolution(table, "ALTER TABLE %s REPLACE COLUMNS (varchar_column varchar(20), tiny_column tinyint, float_column double, int_column bigint)", row("jeden", null, 1.1, 1, 1));
        }
    }

    private void testEvolution(TemporaryHiveTable table, String sql, QueryAssert.Row row)
    {
        if (tryExecuteOnHive(format(sql, table.getName()))) {
            assertThat(onTrino().executeQuery("SELECT * FROM " + table.getName()))
                    .contains(row);
        }
    }

    private static boolean tryExecuteOnHive(String sql)
    {
        try {
            onHive().executeQuery(sql);
            return true;
        }
        catch (QueryExecutionException e) {
            String message = e.getMessage();
            if (message.contains("Unable to alter table. The following columns have types incompatible with the existing columns in their respective positions")
                    || message.contains("Replacing columns cannot drop columns")
                    || message.contains("Replace columns is not supported for")) {
                log.warn("Unable to execute: %s, due: %s", sql, message);
                return false;
            }
            throw e;
        }
    }

    private TemporaryHiveTable createTable(String format)
    {
        String tableName = "schema_evolution_" + randomNameSuffix();
        tryExecuteOnHive(format(
                "CREATE TABLE %s (" +
                        "  int_column int," +
                        "  float_column float," +
                        "  varchar_column varchar(20)" +
                        ") " +
                        "PARTITIONED BY (partition_column bigint) " +
                        "STORED AS %s " +
                        (createTablesAsAcid ? "TBLPROPERTIES ('transactional_properties' = 'none', 'transactional' = 'false')" : ""),
                tableName,
                format));
        TemporaryHiveTable temporaryHiveTable = temporaryHiveTable(tableName);
        try {
            onTrino().executeQuery(format("INSERT INTO %s VALUES (1, 1.1, 'jeden', 1)", tableName));
        }
        catch (Exception e) {
            temporaryHiveTable.closeQuietly(e);
            throw e;
        }
        return temporaryHiveTable;
    }
}
