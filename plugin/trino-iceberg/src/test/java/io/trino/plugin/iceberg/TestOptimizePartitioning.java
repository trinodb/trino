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

import org.junit.jupiter.api.Test;

import static io.trino.plugin.iceberg.IcebergFileFormat.PARQUET;
import static io.trino.testing.TestingNames.randomNameSuffix;

public class TestOptimizePartitioning
        extends BaseIcebergConnectorTest
{
    public TestOptimizePartitioning()
    {
        super(PARQUET);
    }

    @Override
    protected boolean supportsIcebergFileStatistics(String typeName)
    {
        return true;
    }

    @Override
    protected boolean supportsRowGroupStatistics(String typeName)
    {
        return !(typeName.equalsIgnoreCase("varbinary") ||
                typeName.equalsIgnoreCase("time") ||
                typeName.equalsIgnoreCase("time(6)") ||
                typeName.equalsIgnoreCase("timestamp(3) with time zone") ||
                typeName.equalsIgnoreCase("timestamp(6) with time zone"));
    }

    @Override
    protected boolean isFileSorted(String path, String sortColumnName)
    {
        return false;
    }

    @Test
    public void testOptimizeWithIdentityPartitioning()
    {
        String tableName = "test_optimize_identity_" + randomNameSuffix();

        // Create a table with identity partitioning
        assertUpdate("CREATE TABLE " + tableName + " (id bigint, name varchar, region varchar) " +
                "WITH (partitioning = ARRAY['region'])");

        // Insert some data
        assertUpdate("INSERT INTO " + tableName + " VALUES " +
                "(1, 'Alice', 'US'), " +
                "(2, 'Bob', 'US'), " +
                "(3, 'Charlie', 'EU'), " +
                "(4, 'David', 'EU')", 4);

        // Run OPTIMIZE - this should now use partition-aware fan-out
        assertQuerySucceeds("ALTER TABLE " + tableName + " EXECUTE OPTIMIZE");

        // Verify data is still correct after optimization
        assertQuery("SELECT count(*) FROM " + tableName, "VALUES (4)");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testOptimizeWithBucketPartitioning()
    {
        String tableName = "test_optimize_bucket_" + randomNameSuffix();

        // Create a table with bucket partitioning
        assertUpdate("CREATE TABLE " + tableName + " (id bigint, name varchar) " +
                "WITH (partitioning = ARRAY['bucket(id, 4)'])");

        // Insert some data
        assertUpdate("INSERT INTO " + tableName + " VALUES " +
                "(1, 'Alice'), " +
                "(2, 'Bob'), " +
                "(3, 'Charlie'), " +
                "(4, 'David')", 4);

        // Run OPTIMIZE - this should now use partition-aware fan-out
        assertQuerySucceeds("ALTER TABLE " + tableName + " EXECUTE OPTIMIZE");

        // Verify data is still correct after optimization
        assertQuery("SELECT count(*) FROM " + tableName, "VALUES (4)");

        assertUpdate("DROP TABLE " + tableName);
    }
}
