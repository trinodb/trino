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

import io.trino.plugin.hive.containers.Hive4FlociDataLake;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import static io.trino.testing.TestingNames.randomNameSuffix;
import static org.junit.jupiter.api.Assumptions.abort;

@Execution(ExecutionMode.SAME_THREAD) // TODO Make custom hive4 image to support running queries concurrently
class TestHive4OnDataLake
        extends BaseTestHiveOnDataLake
{
    private static final String BUCKET_NAME = "test-hive-insert-overwrite-" + randomNameSuffix();

    public TestHive4OnDataLake()
    {
        super(BUCKET_NAME, new Hive4FlociDataLake(BUCKET_NAME));
    }

    @BeforeAll
    public void setManagedLocation()
    {
        // Hive 4 places ACID tables under the database managed location, which cannot be set from Trino
        hiveFlociDataLake.runOnHive("ALTER DATABASE %1$s SET MANAGEDLOCATION 's3a://%2$s/%1$s_managed'".formatted(HIVE_TEST_SCHEMA, bucketName));
    }

    @Override
    @Test
    public void testSyncPartitionOnBucketRoot()
    {
        // https://github.com/trinodb/trino/issues/24453
        abort("Fails with `location must not be root path`");
    }

    @Override
    @Test
    public void testUnpartitionedTableExternalLocationOnTopOfTheBucket()
    {
        // https://github.com/trinodb/trino/issues/24453
        abort("Fails with `location must not be root path`");
    }

    @Override
    @Test
    public void testPartitionedTableExternalLocationOnTopOfTheBucket()
    {
        // https://github.com/trinodb/trino/issues/24453
        abort("Fails with `location must not be root path`");
    }

    @Override
    @Test
    public void testInsertOverwritePartitionedAndBucketedAcidTable()
    {
        // https://github.com/trinodb/trino/issues/24454
        abort("Fails with `Processor has no capabilities, cannot create an ACID table`");
    }

    @Test
    public void testAcidTableMultipleDereferencesOfSameColumn()
    {
        // Hive 4 metastore rejects ACID table creation from Trino (https://github.com/trinodb/trino/issues/24454),
        // so the table is created through Hive.
        String tableName = "test_acid_dereferences_" + randomNameSuffix();
        String hiveTableName = getHiveTestTableName(tableName);
        String testTable = getFullyQualifiedTestTableName(tableName);
        hiveFlociDataLake.runOnHive(
                "SET hive.support.concurrency=true; " +
                        "SET hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager; " +
                        "CREATE TABLE " + hiveTableName + " (id bigint, row_column struct<nested_a:string, nested_b:string, nested_c:string>, flat_column string) STORED AS ORC TBLPROPERTIES ('transactional'='true'); " +
                        "INSERT INTO " + hiveTableName + " VALUES " +
                        "(1, named_struct('nested_a', 'a1', 'nested_b', 'b1', 'nested_c', 'c1'), 'flat1'), " +
                        "(2, named_struct('nested_a', 'a2', 'nested_b', 'b2', 'nested_c', 'c2'), 'flat2')");
        try {
            assertQuery("SELECT row_column.nested_a, row_column.nested_b, flat_column FROM " + testTable, "VALUES ('a1', 'b1', 'flat1'), ('a2', 'b2', 'flat2')");
            assertQuery("SELECT row_column.nested_a, flat_column FROM " + testTable + " WHERE row_column.nested_b = 'b2'", "VALUES ('a2', 'flat2')");
        }
        finally {
            hiveFlociDataLake.runOnHive("DROP TABLE " + hiveTableName);
        }
    }
}
