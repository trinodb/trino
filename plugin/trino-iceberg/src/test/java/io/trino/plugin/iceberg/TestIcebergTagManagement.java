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

import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;

import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.lang.String.format;

public class TestIcebergTagManagement
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return IcebergQueryRunner.builder().build();
    }

    private long getCurrentSnapshotId(String tableName)
    {
        return (long) computeScalar(
                format("SELECT snapshot_id FROM \"%s$snapshots\" " +
                                "ORDER BY committed_at DESC FETCH FIRST 1 ROW WITH TIES",
                        tableName));
    }

    @Test
    public void testCreateReplaceDropTag()
    {
        String tableName = "test_tag_mgmt_" + randomNameSuffix();
        assertUpdate(format("CREATE TABLE %s (id INT)", tableName));

        // CREATE TAG
        long firstSnapshot = getCurrentSnapshotId(tableName);
        assertUpdate(format("ALTER TABLE %s CREATE TAG my_tag", tableName));
        assertQuery(
                format("SELECT snapshot_id FROM \"%s$refs\" WHERE name = 'my_tag'", tableName),
                format("VALUES %s", firstSnapshot));

        // CREATE TAG IF NOT EXISTS
        assertUpdate(format("ALTER TABLE %s CREATE TAG IF NOT EXISTS my_tag", tableName));
        assertQuery(
                format("SELECT COUNT(*) FROM \"%s$refs\" WHERE name = 'my_tag'", tableName),
                "VALUES 1");

        // Generate new snapshot
        assertUpdate(format("INSERT INTO %s VALUES (1)", tableName), 1);
        long secondSnapshot = getCurrentSnapshotId(tableName);

        // REPLACE TAG
        assertUpdate(format("ALTER TABLE %s REPLACE TAG my_tag FOR VERSION AS OF %s", tableName, secondSnapshot));
        assertQuery(
                format("SELECT snapshot_id FROM \"%s$refs\" WHERE name = 'my_tag'", tableName),
                format("VALUES %s", secondSnapshot));

        // Validate read through tag
        assertQuery(format("SELECT * FROM %s FOR VERSION AS OF 'my_tag'", tableName), "VALUES (1)");

        // DROP TAG
        assertUpdate(format("ALTER TABLE %s DROP TAG my_tag", tableName));
        assertQuery(
                format("SELECT COUNT(*) FROM \"%s$refs\" WHERE name = 'my_tag'", tableName),
                "VALUES 0");
        assertQueryFails(
                format("SELECT * FROM %s FOR VERSION AS OF 'my_tag'", tableName),
                ".*Cannot find snapshot with reference name: my_tag.*");

        assertUpdate(format("DROP TABLE %s", tableName));
    }

    @Test
    public void testTagRetention()
    {
        String tableName = "test_tag_retention_" + randomNameSuffix();
        assertUpdate(format("CREATE TABLE %s (id INT)", tableName));
        assertUpdate(format("ALTER TABLE %s CREATE TAG retained_tag RETAIN 2 DAYS", tableName));

        long expectedMs = 2L * 24 * 60 * 60 * 1000;
        assertQuery(
                format("SELECT max_reference_age_in_ms FROM \"%s$refs\" WHERE name = 'retained_tag'", tableName),
                format("VALUES %s", expectedMs));

        assertUpdate(format("DROP TABLE %s", tableName));
    }

    @Test
    public void testCreateOrReplaceTag()
    {
        String tableName = "test_create_tag_or_replace_" + randomNameSuffix();
        assertUpdate(format("CREATE TABLE %s (id INT)", tableName));
        assertUpdate(format("ALTER TABLE %s CREATE TAG cor_tag", tableName));

        // Generate new snapshot
        assertUpdate(format("INSERT INTO %s VALUES (10)", tableName), 1);
        long newSnapshot = getCurrentSnapshotId(tableName);

        // CREATE OR REPLACE behaves like REPLACE
        assertUpdate(format("ALTER TABLE %s CREATE OR REPLACE TAG cor_tag FOR VERSION AS OF %s", tableName, newSnapshot));
        assertQuery(
                format("SELECT snapshot_id FROM \"%s$refs\" WHERE name = 'cor_tag'", tableName),
                format("VALUES %s", newSnapshot));

        assertUpdate(format("DROP TABLE %s", tableName));
    }

    @Test
    public void testTagErrorCases()
    {
        String tableName = "test_tag_errors_" + randomNameSuffix();
        assertUpdate(format("CREATE TABLE %s (id INT)", tableName));

        // Test duplicate tag creation
        assertUpdate(format("ALTER TABLE %s CREATE TAG existing_tag", tableName));
        assertQueryFails(
                format("ALTER TABLE %s CREATE TAG existing_tag", tableName),
                ".*Failed to create tag 'existing_tag'.*");

        // Test dropping non-existent tag
        assertQueryFails(
                format("ALTER TABLE %s DROP TAG nonexistent_tag", tableName),
                ".*Failed to drop tag 'nonexistent_tag'.*");

        // Test replacing non-existent tag
        assertQueryFails(
                format("ALTER TABLE %s REPLACE TAG nonexistent_tag", tableName),
                ".*Failed to replace tag 'nonexistent_tag'.*");

        assertUpdate(format("DROP TABLE %s", tableName));
    }

    @Test
    public void testTagWithSpecificSnapshot()
    {
        String tableName = "test_tag_snapshot_" + randomNameSuffix();
        assertUpdate(format("CREATE TABLE %s (id INT)", tableName));

        // Insert initial data
        assertUpdate(format("INSERT INTO %s VALUES (1)", tableName), 1);
        long firstSnapshot = getCurrentSnapshotId(tableName);

        // Insert more data
        assertUpdate(format("INSERT INTO %s VALUES (2)", tableName), 1);

        // CREATE TAG pointing to first snapshot
        assertUpdate(format("ALTER TABLE %s CREATE TAG tag_first FOR VERSION AS OF %s", tableName, firstSnapshot));
        assertQuery(
                format("SELECT snapshot_id FROM \"%s$refs\" WHERE name = 'tag_first'", tableName),
                format("VALUES %s", firstSnapshot));

        // Verify tag points to correct data
        assertQuery(format("SELECT * FROM %s FOR VERSION AS OF 'tag_first'", tableName), "VALUES (1)");
        assertQuery(format("SELECT * FROM %s", tableName), "VALUES (1), (2)");

        assertUpdate(format("DROP TABLE %s", tableName));
    }

    @Test
    public void testTagWithRetentionAndSnapshot()
    {
        String tableName = "test_tag_retention_snapshot_" + randomNameSuffix();
        assertUpdate(format("CREATE TABLE %s (id INT)", tableName));

        // Insert data and get snapshot
        assertUpdate(format("INSERT INTO %s VALUES (100)", tableName), 1);
        long snapshot = getCurrentSnapshotId(tableName);

        // CREATE TAG with both retention and specific snapshot
        assertUpdate(format("ALTER TABLE %s CREATE TAG combo_tag FOR VERSION AS OF %s RETAIN 7 DAYS", tableName, snapshot));

        // Verify both snapshot and retention are set correctly
        assertQuery(
                format("SELECT snapshot_id FROM \"%s$refs\" WHERE name = 'combo_tag'", tableName),
                format("VALUES %s", snapshot));

        long expectedRetentionMs = 7L * 24 * 60 * 60 * 1000;
        assertQuery(
                format("SELECT max_reference_age_in_ms FROM \"%s$refs\" WHERE name = 'combo_tag'", tableName),
                format("VALUES %s", expectedRetentionMs));

        // Verify reading through tag
        assertQuery(format("SELECT * FROM %s FOR VERSION AS OF 'combo_tag'", tableName), "VALUES (100)");

        assertUpdate(format("DROP TABLE %s", tableName));
    }
}
