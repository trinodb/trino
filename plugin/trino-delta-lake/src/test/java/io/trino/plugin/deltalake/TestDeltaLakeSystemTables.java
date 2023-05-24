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
package io.trino.plugin.deltalake;

import com.google.common.collect.ImmutableMap;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import org.testng.annotations.Test;

import static io.trino.plugin.deltalake.DeltaLakeQueryRunner.DELTA_CATALOG;
import static io.trino.plugin.deltalake.DeltaLakeQueryRunner.createDeltaLakeQueryRunner;
import static org.assertj.core.api.Assertions.assertThat;

public class TestDeltaLakeSystemTables
        extends AbstractTestQueryFramework
{
    @Override
    protected DistributedQueryRunner createQueryRunner()
            throws Exception
    {
        return createDeltaLakeQueryRunner(
                DELTA_CATALOG,
                ImmutableMap.of(),
                ImmutableMap.of("delta.enable-non-concurrent-writes", "true"));
    }

    @Test
    public void testHistoryTable()
    {
        try {
            assertUpdate("CREATE TABLE test_simple_table (_bigint BIGINT)");
            assertUpdate("INSERT INTO test_simple_table VALUES 1, 2, 3", 3);
            assertQuery("SELECT count(*) FROM test_simple_table", "VALUES 3");

            assertUpdate("CREATE TABLE test_checkpoint_table (_bigint BIGINT, _date DATE) WITH (partitioned_by = ARRAY['_date'] )");
            assertUpdate("INSERT INTO test_checkpoint_table VALUES (0, CAST('2019-09-08' AS DATE)), (1, CAST('2019-09-09' AS DATE)), (2, CAST('2019-09-09' AS DATE))", 3);
            assertUpdate("INSERT INTO test_checkpoint_table VALUES (3, CAST('2019-09-09' AS DATE)), (4, CAST('2019-09-10' AS DATE)), (5, CAST('2019-09-10' AS DATE))", 3);
            assertUpdate("UPDATE test_checkpoint_table SET _bigint = 50 WHERE _bigint =  BIGINT '5'", 1);
            assertUpdate("DELETE FROM test_checkpoint_table WHERE _date =  DATE '2019-09-08'", 1);
            assertQuerySucceeds("ALTER TABLE test_checkpoint_table EXECUTE OPTIMIZE");
            assertQuery("SELECT count(*) FROM test_checkpoint_table", "VALUES 5");

            assertQuery("SHOW COLUMNS FROM \"test_checkpoint_table$history\"",
                    """
                            VALUES
                            ('version', 'bigint', '', ''),
                            ('timestamp', 'timestamp(3) with time zone', '', ''),
                            ('user_id', 'varchar', '', ''),
                            ('user_name', 'varchar', '', ''),
                            ('operation', 'varchar', '', ''),
                            ('operation_parameters', 'map(varchar, varchar)', '', ''),
                            ('cluster_id', 'varchar', '', ''),
                            ('read_version', 'bigint', '', ''),
                            ('isolation_level', 'varchar', '', ''),
                            ('is_blind_append', 'boolean', '', '')
                            """);

            // Test the contents of history system table
            assertThat(query("SELECT version, operation, read_version, isolation_level, is_blind_append FROM \"test_simple_table$history\""))
                    .matches("""
                            VALUES
                                (BIGINT '1', VARCHAR 'WRITE', BIGINT '0', VARCHAR 'WriteSerializable', true),
                                (BIGINT '0', VARCHAR 'CREATE TABLE', BIGINT '0', VARCHAR 'WriteSerializable', true)
                            """);
            assertThat(query("SELECT version, operation, read_version, isolation_level, is_blind_append FROM \"test_checkpoint_table$history\""))
                    // TODO (https://github.com/trinodb/trino/issues/15763) Use correct operation name for DML statements
                    .matches("""
                            VALUES
                                (BIGINT '5', VARCHAR 'OPTIMIZE', BIGINT '4', VARCHAR 'WriteSerializable', true),
                                (BIGINT '4', VARCHAR 'MERGE', BIGINT '3', VARCHAR 'WriteSerializable', true),
                                (BIGINT '3', VARCHAR 'MERGE', BIGINT '2', VARCHAR 'WriteSerializable', true),
                                (BIGINT '2', VARCHAR 'WRITE', BIGINT '1', VARCHAR 'WriteSerializable', true),
                                (BIGINT '1', VARCHAR 'WRITE', BIGINT '0', VARCHAR 'WriteSerializable', true),
                                (BIGINT '0', VARCHAR 'CREATE TABLE', BIGINT '0', VARCHAR 'WriteSerializable', true)
                            """);
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS test_simple_table");
            assertUpdate("DROP TABLE IF EXISTS test_checkpoint_table");
        }
    }
}
