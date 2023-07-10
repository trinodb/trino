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
package io.trino.faulttolerant;

import io.trino.Session;
import io.trino.testing.AbstractTestQueryFramework;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.Test;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertEquals;

public abstract class BaseFaultTolerantExecutionTest
        extends AbstractTestQueryFramework
{
    private final String partitioningTablePropertyName;

    protected BaseFaultTolerantExecutionTest(String partitioningTablePropertyName)
    {
        this.partitioningTablePropertyName = requireNonNull(partitioningTablePropertyName, "partitioningTablePropertyName is null");
    }

    @Test
    public void testTableWritePreferredWritePartitioningSkewMitigation()
    {
        @Language("SQL") String createTableSql = """
                CREATE TABLE test_table_writer_skew_mitigation WITH (%s = ARRAY['returnflag']) AS
                SELECT orderkey, partkey, suppkey, linenumber, quantity, extendedprice, discount, tax, linestatus, shipdate, commitdate, receiptdate, shipinstruct, shipmode, comment, returnflag
                FROM tpch.sf1.lineitem
                WHERE returnflag = 'N'
                LIMIT 1000000""".formatted(partitioningTablePropertyName);
        @Language("SQL") String selectFileInfo = "SELECT distinct \"$path\" FROM test_table_writer_skew_mitigation";

        Session session = withSingleWriterPerTask(getSession());

        // force single writer task to verify there is exactly one writer per task
        assertUpdate(withUnlimitedTargetTaskInputSize(session), createTableSql, 1000000);
        assertEquals(computeActual(selectFileInfo).getRowCount(), 1);
        assertUpdate("DROP TABLE test_table_writer_skew_mitigation");

        assertUpdate(withDisabledPreferredWritePartitioning(session), createTableSql, 1000000);
        int expectedNumberOfFiles = computeActual(selectFileInfo).getRowCount();
        assertUpdate("DROP TABLE test_table_writer_skew_mitigation");
        assertThat(expectedNumberOfFiles).isGreaterThan(1);

        assertUpdate(withEnabledPreferredWritePartitioning(session), createTableSql, 1000000);
        int actualNumberOfFiles = computeActual(selectFileInfo).getRowCount();
        assertUpdate("DROP TABLE test_table_writer_skew_mitigation");
        assertEquals(actualNumberOfFiles, expectedNumberOfFiles);
    }

    @Test
    public void testExecutePreferredWritePartitioningSkewMitigation()
    {
        @Language("SQL") String createTableSql = """
                CREATE TABLE test_execute_skew_mitigation WITH (%s = ARRAY['returnflag']) AS
                SELECT orderkey, partkey, suppkey, linenumber, quantity, extendedprice, discount, tax, linestatus, shipdate, commitdate, receiptdate, shipinstruct, shipmode, returnflag
                FROM tpch.sf1.lineitem
                WHERE returnflag = 'N'
                LIMIT 1000000""".formatted(partitioningTablePropertyName);
        assertUpdate(createTableSql, 1000000);

        @Language("SQL") String executeSql = "ALTER TABLE test_execute_skew_mitigation EXECUTE optimize";
        @Language("SQL") String selectFileInfo = "SELECT distinct \"$path\" FROM test_execute_skew_mitigation";

        Session session = withSingleWriterPerTask(getSession());

        // force single writer task to verify there is exactly one writer per task
        assertUpdate(withUnlimitedTargetTaskInputSize(session), executeSql);
        assertEquals(computeActual(selectFileInfo).getRowCount(), 1);

        assertUpdate(withDisabledPreferredWritePartitioning(session), executeSql);
        int expectedNumberOfFiles = computeActual(selectFileInfo).getRowCount();
        assertThat(expectedNumberOfFiles)
                .withFailMessage("optimize is expected to generate more than a single file per partition")
                .isGreaterThan(1);

        assertUpdate(withEnabledPreferredWritePartitioning(session), executeSql);
        int actualNumberOfFiles = computeActual(selectFileInfo).getRowCount();
        assertEquals(actualNumberOfFiles, expectedNumberOfFiles);

        // verify no data is lost in process
        assertQuery("SELECT count(*) FROM test_execute_skew_mitigation", "SELECT 1000000");

        assertUpdate("DROP TABLE test_execute_skew_mitigation");
    }

    private static Session withSingleWriterPerTask(Session session)
    {
        return Session.builder(session)
                // one writer per partition per task
                .setSystemProperty("task_writer_count", "1")
                .setSystemProperty("task_partitioned_writer_count", "1")
                .setSystemProperty("task_scale_writers_enabled", "false")
                .build();
    }

    private static Session withUnlimitedTargetTaskInputSize(Session session)
    {
        return Session.builder(session)
                .setSystemProperty("fault_tolerant_execution_arbitrary_distribution_compute_task_target_size_min", "1PB")
                .setSystemProperty("fault_tolerant_execution_arbitrary_distribution_compute_task_target_size_max", "1PB")
                .setSystemProperty("fault_tolerant_execution_arbitrary_distribution_write_task_target_size_min", "1PB")
                .setSystemProperty("fault_tolerant_execution_arbitrary_distribution_write_task_target_size_max", "1PB")
                .setSystemProperty("fault_tolerant_execution_hash_distribution_compute_task_target_size", "1PB")
                .setSystemProperty("fault_tolerant_execution_hash_distribution_write_task_target_size", "1PB")
                .build();
    }

    private static Session withDisabledPreferredWritePartitioning(Session session)
    {
        return Session.builder(session)
                .setSystemProperty("use_preferred_write_partitioning", "false")
                .build();
    }

    private static Session withEnabledPreferredWritePartitioning(Session session)
    {
        return Session.builder(session)
                .setSystemProperty("use_preferred_write_partitioning", "true")
                .build();
    }
}
