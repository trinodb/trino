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

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

class TestIcebergMaterializedViewsSystemTables
        extends BaseIcebergSystemTables
{
    TestIcebergMaterializedViewsSystemTables()
    {
        super(IcebergFileFormat.PARQUET);
    }

    @BeforeAll
    @Override
    public void setUp()
    {
        assertUpdate("CREATE SCHEMA test_schema");

        assertUpdate("CREATE TABLE test_schema.test_table_storage (_bigint BIGINT, _date DATE) WITH (partitioning = ARRAY['_date'])");
        assertUpdate("CREATE MATERIALIZED VIEW test_schema.test_table WITH (partitioning = ARRAY['_date']) AS SELECT * FROM test_schema.test_table_storage");
        assertUpdate("INSERT INTO test_schema.test_table_storage VALUES (0, CAST('2019-09-08' AS DATE)), (1, CAST('2019-09-09' AS DATE)), (2, CAST('2019-09-09' AS DATE))", 3);
        assertUpdate("REFRESH MATERIALIZED VIEW test_schema.test_table", 3);
        assertUpdate("INSERT INTO test_schema.test_table_storage VALUES (3, CAST('2019-09-09' AS DATE)), (4, CAST('2019-09-10' AS DATE)), (5, CAST('2019-09-10' AS DATE))", 3);
        assertUpdate("REFRESH MATERIALIZED VIEW test_schema.test_table", 3);
        assertQuery("SELECT count(*) FROM test_schema.test_table", "VALUES 6");

        assertUpdate("CREATE TABLE test_schema.test_table_multilevel_partitions_storage (_varchar VARCHAR, _bigint BIGINT, _date DATE) WITH (partitioning = ARRAY['_bigint', '_date'])");
        assertUpdate("CREATE MATERIALIZED VIEW test_schema.test_table_multilevel_partitions WITH (partitioning = ARRAY['_bigint', '_date']) AS SELECT * FROM test_schema.test_table_multilevel_partitions_storage");
        assertUpdate("INSERT INTO test_schema.test_table_multilevel_partitions_storage VALUES ('a', 0, CAST('2019-09-08' AS DATE)), ('a', 1, CAST('2019-09-08' AS DATE)), ('a', 0, CAST('2019-09-09' AS DATE))", 3);
        assertUpdate("REFRESH MATERIALIZED VIEW test_schema.test_table_multilevel_partitions", 3);
        assertQuery("SELECT count(*) FROM test_schema.test_table_multilevel_partitions", "VALUES 3");

        assertUpdate("CREATE TABLE test_schema.test_table_nan_storage (_bigint BIGINT, _double DOUBLE, _real REAL, _date DATE) WITH (partitioning = ARRAY['_date'])");
        assertUpdate("CREATE MATERIALIZED VIEW test_schema.test_table_nan WITH (partitioning = ARRAY['_date']) AS SELECT * FROM test_schema.test_table_nan_storage");
        assertUpdate("INSERT INTO test_schema.test_table_nan_storage VALUES (1, 1.1, 1.2, CAST('2022-01-01' AS DATE)), (2, nan(), 2.2, CAST('2022-01-02' AS DATE)), (3, 3.3, nan(), CAST('2022-01-03' AS DATE))", 3);
        assertUpdate("REFRESH MATERIALIZED VIEW test_schema.test_table_nan", 3);
        assertUpdate("INSERT INTO test_schema.test_table_nan_storage VALUES (4, nan(), 4.1, CAST('2022-01-04' AS DATE)), (5, 4.2, nan(), CAST('2022-01-04' AS DATE)), (6, nan(), nan(), CAST('2022-01-04' AS DATE))", 3);
        assertUpdate("REFRESH MATERIALIZED VIEW test_schema.test_table_nan", 3);
        assertQuery("SELECT count(*) FROM test_schema.test_table_nan", "VALUES 6");
    }

    @Test
    @Override
    public void testSnapshotsTable()
    {
        assertQuery("SHOW COLUMNS FROM test_schema.\"test_table$snapshots\"",
                "VALUES ('committed_at', 'timestamp(3) with time zone', '', '')," +
                        "('snapshot_id', 'bigint', '', '')," +
                        "('parent_id', 'bigint', '', '')," +
                        "('operation', 'varchar', '', '')," +
                        "('manifest_list', 'varchar', '', '')," +
                        "('summary', 'map(varchar, varchar)', '', '')");

        // For a Materialized view, the first operation is a "delete"
        assertQuery("SELECT operation FROM test_schema.\"test_table$snapshots\"", "VALUES 'delete', 'append', 'append'");
        assertQuery("SELECT summary['total-records'] FROM test_schema.\"test_table$snapshots\"", "VALUES '0', '3', '6'");
    }

    @Test
    @Disabled("Materialized views don't support dropping of columns")
    @Override
    public void testPartitionTableOnDropColumn()
    { }

    @Test
    @Disabled("Materialized views don't support dropping of columns")
    @Override
    public void testFilesTableOnDropColumn()
    { }

    @Test
    @Disabled("Materialized views don't support incremental updates")
    @Override
    public void testDMLManifestsTable()
    { }

    @Test
    @Disabled("Materialized views don't include some file statistics")
    @Override
    public void testSplitOffsetsFilesTable()
    { }
}
