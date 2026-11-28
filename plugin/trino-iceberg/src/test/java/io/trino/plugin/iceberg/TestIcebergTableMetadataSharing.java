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
package io.trino.plugin.iceberg;

import io.trino.Session;
import io.trino.metadata.Metadata;
import io.trino.metadata.QualifiedObjectName;
import io.trino.spi.transaction.IsolationLevel;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import io.trino.transaction.TransactionId;
import io.trino.transaction.TransactionManager;
import org.apache.iceberg.Transaction;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Isolated;

import javax.management.ObjectName;

import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.List;

import static io.trino.plugin.iceberg.IcebergQueryRunner.ICEBERG_CATALOG;
import static io.trino.plugin.iceberg.IcebergTestUtils.getFileSystemFactory;
import static io.trino.plugin.iceberg.IcebergTestUtils.getHiveMetastore;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static org.assertj.core.api.Assertions.assertThat;

// Counts live objects in the whole JVM, so no other test may run concurrently
@Isolated
public class TestIcebergTableMetadataSharing
        extends AbstractTestQueryFramework
{
    private static final int SNAPSHOTS = 1000;
    private static final int TRANSACTIONS = 20;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return IcebergQueryRunner.builder().build();
    }

    @Test
    public void testConcurrentTransactionsShareParsedTableMetadata()
            throws Exception
    {
        String tableName = "test_table_metadata_sharing_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (x bigint)");
        addEmptySnapshots(tableName, SNAPSHOTS);

        TransactionManager transactionManager = getQueryRunner().getTransactionManager();
        Metadata metadata = getQueryRunner().getPlannerContext().getMetadata();
        QualifiedObjectName name = new QualifiedObjectName(ICEBERG_CATALOG, "tpch", tableName);

        Histogram before = Histogram.live();
        List<TransactionId> transactions = new ArrayList<>();
        try {
            // Each open transaction stands for one running query that has analyzed the table
            for (int i = 0; i < TRANSACTIONS; i++) {
                TransactionId transactionId = transactionManager.beginTransaction(IsolationLevel.READ_UNCOMMITTED, true, false);
                transactions.add(transactionId);
                Session session = getSession().beginTransactionId(transactionId, transactionManager, getQueryRunner().getAccessControl());
                assertThat(metadata.getTableHandle(session, name)).isPresent();
            }
            Histogram after = Histogram.live();

            long tableMetadataCopies = after.instances("org.apache.iceberg.TableMetadata") - before.instances("org.apache.iceberg.TableMetadata");
            long snapshots = after.instances("org.apache.iceberg.BaseSnapshot") - before.instances("org.apache.iceberg.BaseSnapshot");
            long retainedBytes = after.totalBytes() - before.totalBytes();
            String report = "%d open transactions on a table with %d snapshots retain %d TableMetadata, %d BaseSnapshot, %,d bytes"
                    .formatted(TRANSACTIONS, SNAPSHOTS + 1, tableMetadataCopies, snapshots, retainedBytes);
            System.out.println(report);

            assertThat(tableMetadataCopies)
                    .describedAs(report)
                    .isEqualTo(1);
        }
        finally {
            transactions.forEach(transactionManager::asyncAbort);
            assertUpdate("DROP TABLE " + tableName);
        }
    }

    // A single Iceberg transaction writes one metadata file that lists all snapshots
    private void addEmptySnapshots(String tableName, int count)
    {
        Transaction transaction = IcebergTestUtils.loadTable(tableName, getHiveMetastore(getQueryRunner()), getFileSystemFactory(getQueryRunner()), ICEBERG_CATALOG, "tpch")
                .newTransaction();
        for (int i = 0; i < count; i++) {
            transaction.newFastAppend().commit();
        }
        transaction.commitTransaction();
    }

    private record Histogram(String text)
    {
        // Full GC followed by a class histogram of live objects, the same data a heap dump analyzer reads
        static Histogram live()
                throws Exception
        {
            return new Histogram((String) ManagementFactory.getPlatformMBeanServer().invoke(
                    new ObjectName("com.sun.management:type=DiagnosticCommand"),
                    "gcClassHistogram",
                    new Object[] {new String[0]},
                    new String[] {String[].class.getName()}));
        }

        long instances(String className)
        {
            return text.lines()
                    .map(line -> line.trim().split("\\s+"))
                    .filter(columns -> columns.length >= 4 && columns[3].equals(className))
                    .mapToLong(columns -> Long.parseLong(columns[1]))
                    .sum();
        }

        long totalBytes()
        {
            return text.lines()
                    .map(line -> line.trim().split("\\s+"))
                    .filter(columns -> columns.length == 3 && columns[0].equals("Total"))
                    .mapToLong(columns -> Long.parseLong(columns[2]))
                    .findFirst()
                    .orElseThrow();
        }
    }
}
