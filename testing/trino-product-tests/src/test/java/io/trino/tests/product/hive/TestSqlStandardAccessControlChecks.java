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

import io.trino.testing.containers.environment.ProductTest;
import io.trino.testing.containers.environment.RequiresEnvironment;
import io.trino.tests.product.TestGroup;
import org.junit.jupiter.api.Test;

import static io.trino.plugin.hive.HiveMetadata.MODIFYING_NON_TRANSACTIONAL_TABLE_MESSAGE;
import static io.trino.testing.containers.environment.QueryResultAssert.assertThat;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * JUnit 5 port of TestSqlStandardAccessControlChecks.
 * <p>
 * Tests SQL standard access control checks for Hive tables and views
 * in a Kerberos environment with impersonation enabled.
 */
@ProductTest
@RequiresEnvironment(HiveKerberosImpersonationEnvironment.class)
@TestGroup.Authorization
class TestSqlStandardAccessControlChecks
{
    private static final String TABLE_NAME = "alice_owned_table";
    private static final String VIEW_NAME = "alice_owned_view";

    // User names for tests
    private static final String ALICE = "alice";
    private static final String BOB = "bob";
    private static final String CHARLIE = "charlie";
    private static final String CASE_SENSITIVE_USER = "CaseSensitiveUserName";
    private static final String HDFS = "hdfs";

    @Test
    void testAccessControlSelect(HiveKerberosImpersonationEnvironment env)
    {
        String tableName = TABLE_NAME + "_select";
        String viewName = VIEW_NAME + "_select";
        try {
            // Setup: Alice creates table and view
            env.executeTrinoUpdateAs(ALICE, format("DROP TABLE IF EXISTS %s", tableName));
            env.executeTrinoUpdateAs(ALICE, format("CREATE TABLE %s(month bigint, day bigint) WITH (partitioned_by = ARRAY['day'])", tableName));
            env.executeTrinoUpdateAs(ALICE, format("DROP VIEW IF EXISTS %s", viewName));
            env.executeTrinoUpdateAs(ALICE, format("CREATE VIEW %s AS SELECT month, day FROM %s", viewName, tableName));

            // Bob cannot select from Alice's table
            assertThatThrownBy(() -> env.executeTrinoAs(BOB, format("SELECT * FROM %s", tableName)))
                    .hasMessageContaining("Access Denied: Cannot select from table default.%s", tableName);

            // Alice grants SELECT to Bob
            env.executeTrinoUpdateAs(ALICE, format("GRANT SELECT ON %s TO bob", tableName));
            assertThat(env.executeTrinoAs(BOB, format("SELECT * FROM %s", tableName))).hasNoRows();

            // Bob cannot select from Alice's view
            assertThatThrownBy(() -> env.executeTrinoAs(BOB, format("SELECT * FROM %s", viewName)))
                    .hasMessageContaining("Access Denied: Cannot select from table default.%s", viewName);

            // Alice grants SELECT on view to Bob
            env.executeTrinoUpdateAs(ALICE, format("GRANT SELECT ON %s TO bob", viewName));
            assertThat(env.executeTrinoAs(BOB, format("SELECT * FROM %s", viewName))).hasNoRows();
        }
        finally {
            env.executeTrinoUpdateAs(ALICE, format("DROP VIEW IF EXISTS %s", viewName));
            env.executeTrinoUpdateAs(ALICE, format("DROP TABLE IF EXISTS %s", tableName));
        }
    }

    @Test
    void testAccessControlSelectFromPartitions(HiveKerberosImpersonationEnvironment env)
    {
        String tableName = TABLE_NAME + "_partitions";
        try {
            // Setup: Alice creates table
            env.executeTrinoUpdateAs(ALICE, format("DROP TABLE IF EXISTS %s", tableName));
            env.executeTrinoUpdateAs(ALICE, format("CREATE TABLE %s(month bigint, day bigint) WITH (partitioned_by = ARRAY['day'])", tableName));

            // Bob cannot select from partitions metadata
            assertThatThrownBy(() -> env.executeTrinoAs(BOB, format("SELECT * FROM \"%s$partitions\"", tableName)))
                    .hasMessageContaining("Access Denied: Cannot select from table default.%s$partitions", tableName);

            // Alice grants SELECT to Bob
            env.executeTrinoUpdateAs(ALICE, format("GRANT SELECT ON %s TO bob", tableName));
            assertThat(env.executeTrinoAs(BOB, format("SELECT * FROM \"%s$partitions\"", tableName))).hasNoRows();
        }
        finally {
            env.executeTrinoUpdateAs(ALICE, format("DROP TABLE IF EXISTS %s", tableName));
        }
    }

    @Test
    void testAccessControlInsert(HiveKerberosImpersonationEnvironment env)
    {
        String tableName = TABLE_NAME + "_insert";
        try {
            // Setup: Alice creates table
            env.executeTrinoUpdateAs(ALICE, format("DROP TABLE IF EXISTS %s", tableName));
            env.executeTrinoUpdateAs(ALICE, format("CREATE TABLE %s(month bigint, day bigint) WITH (partitioned_by = ARRAY['day'])", tableName));

            // Bob cannot insert into Alice's table
            assertThatThrownBy(() -> env.executeTrinoUpdateAs(BOB, format("INSERT INTO %s VALUES (3, 22)", tableName)))
                    .hasMessageContaining("Access Denied: Cannot insert into table default.%s", tableName);

            // Alice grants INSERT to Bob
            env.executeTrinoUpdateAs(ALICE, format("GRANT INSERT ON %s TO bob", tableName));
            env.executeTrinoUpdateAs(BOB, format("INSERT INTO %s VALUES (3, 22)", tableName));
            assertThat(env.executeTrinoAs(ALICE, format("SELECT * FROM %s", tableName))).hasRowsCount(1);
        }
        finally {
            env.executeTrinoUpdateAs(ALICE, format("DROP TABLE IF EXISTS %s", tableName));
        }
    }

    @Test
    void testAccessControlUpdate(HiveKerberosImpersonationEnvironment env)
    {
        String tableName = TABLE_NAME + "_update";
        try {
            // Setup: Alice creates table
            env.executeTrinoUpdateAs(ALICE, format("DROP TABLE IF EXISTS %s", tableName));
            env.executeTrinoUpdateAs(ALICE, format("CREATE TABLE %s(month bigint, day bigint) WITH (partitioned_by = ARRAY['day'])", tableName));

            // Bob cannot update Alice's table
            assertThatThrownBy(() -> env.executeTrinoUpdateAs(BOB, format("UPDATE %s SET month=3, day=22", tableName)))
                    .hasMessageContaining("Access Denied: Cannot update columns [month, day] in table default.%s", tableName);

            // Alice grants INSERT to Bob - still cannot update
            env.executeTrinoUpdateAs(ALICE, format("GRANT INSERT ON %s TO bob", tableName));
            assertThatThrownBy(() -> env.executeTrinoUpdateAs(BOB, format("UPDATE %s SET month=3, day=22", tableName)))
                    .hasMessageContaining("Access Denied: Cannot update columns [month, day] in table default.%s", tableName);

            // Alice grants UPDATE to Bob - now fails because table is non-transactional
            env.executeTrinoUpdateAs(ALICE, format("GRANT UPDATE ON %s TO bob", tableName));
            assertThatThrownBy(() -> env.executeTrinoUpdateAs(BOB, format("UPDATE %s SET month=3, day=22", tableName)))
                    .hasMessageContaining(MODIFYING_NON_TRANSACTIONAL_TABLE_MESSAGE);
        }
        finally {
            env.executeTrinoUpdateAs(ALICE, format("DROP TABLE IF EXISTS %s", tableName));
        }
    }

    @Test
    void testAccessControlDelete(HiveKerberosImpersonationEnvironment env)
    {
        String tableName = TABLE_NAME + "_delete";
        try {
            // Setup: Alice creates table and inserts data
            env.executeTrinoUpdateAs(ALICE, format("DROP TABLE IF EXISTS %s", tableName));
            env.executeTrinoUpdateAs(ALICE, format("CREATE TABLE %s(month bigint, day bigint) WITH (partitioned_by = ARRAY['day'])", tableName));
            env.executeTrinoUpdateAs(ALICE, format("INSERT INTO %s VALUES (4, 13)", tableName));
            assertThat(env.executeTrinoAs(ALICE, format("SELECT * FROM %s", tableName))).hasRowsCount(1);

            // Bob cannot delete from Alice's table
            assertThatThrownBy(() -> env.executeTrinoUpdateAs(BOB, format("DELETE FROM %s WHERE day=4", tableName)))
                    .hasMessageContaining("Access Denied: Cannot delete from table default.%s", tableName);

            // Alice grants DELETE to Bob
            env.executeTrinoUpdateAs(ALICE, format("GRANT DELETE ON %s TO bob", tableName));
            env.executeTrinoUpdateAs(BOB, format("DELETE FROM %s", tableName));
            assertThat(env.executeTrinoAs(ALICE, format("SELECT * FROM %s", tableName))).hasNoRows();
        }
        finally {
            env.executeTrinoUpdateAs(ALICE, format("DROP TABLE IF EXISTS %s", tableName));
        }
    }

    @Test
    void testAccessControlCreateTableAsSelect(HiveKerberosImpersonationEnvironment env)
    {
        String tableName = TABLE_NAME + "_ctas_source";
        String createTableAsSelect = "bob_create_table_as_select";
        try {
            // Setup: Alice creates source table
            env.executeTrinoUpdateAs(ALICE, format("DROP TABLE IF EXISTS %s", tableName));
            env.executeTrinoUpdateAs(ALICE, format("CREATE TABLE %s(month bigint, day bigint) WITH (partitioned_by = ARRAY['day'])", tableName));

            env.executeTrinoUpdateAs(BOB, "DROP TABLE IF EXISTS " + createTableAsSelect);

            // Bob cannot create table from Alice's table
            assertThatThrownBy(() -> env.executeTrinoUpdateAs(BOB, format("CREATE TABLE %s AS SELECT * FROM %s", createTableAsSelect, tableName)))
                    .hasMessageContaining("Access Denied: Cannot select from table default.%s", tableName);

            // Alice grants SELECT to Bob
            env.executeTrinoUpdateAs(ALICE, format("GRANT SELECT ON %s TO bob", tableName));
            env.executeTrinoUpdateAs(BOB, format("CREATE TABLE %s AS SELECT * FROM %s", createTableAsSelect, tableName));
            assertThat(env.executeTrinoAs(BOB, format("SELECT * FROM %s", createTableAsSelect))).hasNoRows();
        }
        finally {
            env.executeTrinoUpdateAs(BOB, "DROP TABLE IF EXISTS " + createTableAsSelect);
            env.executeTrinoUpdateAs(ALICE, format("DROP TABLE IF EXISTS %s", tableName));
        }
    }

    @Test
    void testAccessControlDropTable(HiveKerberosImpersonationEnvironment env)
    {
        String tableName = TABLE_NAME + "_drop";
        // Setup: Alice creates table
        env.executeTrinoUpdateAs(ALICE, format("DROP TABLE IF EXISTS %s", tableName));
        env.executeTrinoUpdateAs(ALICE, format("CREATE TABLE %s(month bigint, day bigint) WITH (partitioned_by = ARRAY['day'])", tableName));

        // Bob cannot drop Alice's table
        assertThatThrownBy(() -> env.executeTrinoUpdateAs(BOB, format("DROP TABLE %s", tableName)))
                .hasMessageContaining("Access Denied: Cannot drop table default.%s", tableName);

        // Alice can drop her own table
        env.executeTrinoUpdateAs(ALICE, format("DROP TABLE %s", tableName));
        assertThatThrownBy(() -> env.executeTrinoAs(ALICE, format("SELECT * FROM %s", tableName)))
                .hasMessageContaining("does not exist");
    }

    @Test
    void testAccessControlAlterTable(HiveKerberosImpersonationEnvironment env)
    {
        String tableName = TABLE_NAME + "_alter";
        try {
            // Setup: Alice creates table
            env.executeTrinoUpdateAs(ALICE, format("DROP TABLE IF EXISTS %s", tableName));
            env.executeTrinoUpdateAs(ALICE, format("CREATE TABLE %s(month bigint, day bigint) WITH (partitioned_by = ARRAY['day'])", tableName));

            assertThat(env.executeTrinoAs(ALICE, format("SHOW COLUMNS FROM %s", tableName))).hasRowsCount(2);

            // Bob cannot alter Alice's table
            assertThatThrownBy(() -> env.executeTrinoUpdateAs(BOB, format("ALTER TABLE %s ADD COLUMN year bigint", tableName)))
                    .hasMessageContaining("Access Denied: Cannot add a column to table default.%s", tableName);

            // Alice can alter her own table
            env.executeTrinoUpdateAs(ALICE, format("ALTER TABLE %s ADD COLUMN year bigint", tableName));
            assertThat(env.executeTrinoAs(ALICE, format("SHOW COLUMNS FROM %s", tableName))).hasRowsCount(3);
        }
        finally {
            env.executeTrinoUpdateAs(ALICE, format("DROP TABLE IF EXISTS %s", tableName));
        }
    }

    @Test
    void testAccessControlCreateView(HiveKerberosImpersonationEnvironment env)
    {
        String tableName = TABLE_NAME + "_view_source";
        String viewName = "bob_view";
        try {
            // Setup: Alice creates table
            env.executeTrinoUpdateAs(ALICE, format("DROP TABLE IF EXISTS %s", tableName));
            env.executeTrinoUpdateAs(ALICE, format("CREATE TABLE %s(month bigint, day bigint) WITH (partitioned_by = ARRAY['day'])", tableName));

            String selectTableSql = format("SELECT * FROM %s", tableName);
            String createViewSql = format("CREATE VIEW %s AS %s", viewName, selectTableSql);

            env.executeTrinoUpdateAs(BOB, format("DROP VIEW IF EXISTS %s", viewName));

            // Bob needs SELECT on the table to create the view
            assertThatThrownBy(() -> env.executeTrinoUpdateAs(BOB, createViewSql))
                    .hasMessageContaining("Access Denied: Cannot select from table default.%s", tableName);

            // Give Bob access to table, then create and execute view
            env.executeTrinoUpdateAs(ALICE, format("GRANT SELECT ON %s TO bob", tableName));
            env.executeTrinoUpdateAs(BOB, createViewSql);
            assertThat(env.executeTrinoAs(BOB, format("SELECT * FROM %s", viewName))).hasNoRows();

            // Verify that Charlie does not have SELECT on the view, then grant access
            assertThatThrownBy(() -> env.executeTrinoAs(CHARLIE, format("SELECT * FROM %s", viewName)))
                    .hasMessageContaining("Access Denied: Cannot select from table default.%s", viewName);
            env.executeTrinoUpdateAs(BOB, format("GRANT SELECT ON %s TO charlie", viewName));

            // Charlie still cannot access view because Bob does not have SELECT WITH GRANT OPTION
            assertThatThrownBy(() -> env.executeTrinoAs(CHARLIE, format("SELECT * FROM %s", viewName)))
                    .hasMessageContaining("Access Denied: View owner does not have sufficient privileges: View owner 'bob' cannot create view that selects from default.%s", tableName);

            // Give Bob SELECT WITH GRANT OPTION on the underlying table
            env.executeTrinoUpdateAs(ALICE, format("REVOKE SELECT ON %s FROM bob", tableName));
            env.executeTrinoUpdateAs(ALICE, format("GRANT SELECT ON %s TO bob WITH GRANT OPTION", tableName));

            // Bob has GRANT OPTION, so both Bob and Charlie can access the view
            assertThat(env.executeTrinoAs(BOB, format("SELECT * FROM %s", viewName))).hasNoRows();
            assertThat(env.executeTrinoAs(CHARLIE, format("SELECT * FROM %s", viewName))).hasNoRows();
        }
        finally {
            env.executeTrinoUpdateAs(BOB, "DROP VIEW IF EXISTS " + viewName);
            env.executeTrinoUpdateAs(ALICE, format("DROP TABLE IF EXISTS %s", tableName));
        }
    }

    @Test
    void testAccessControlDropView(HiveKerberosImpersonationEnvironment env)
    {
        String tableName = TABLE_NAME + "_drop_view_source";
        String viewName = "alice_view_for_drop";
        try {
            // Setup: Alice creates table and view
            env.executeTrinoUpdateAs(ALICE, format("DROP TABLE IF EXISTS %s", tableName));
            env.executeTrinoUpdateAs(ALICE, format("CREATE TABLE %s(month bigint, day bigint) WITH (partitioned_by = ARRAY['day'])", tableName));

            env.executeTrinoUpdateAs(ALICE, "DROP VIEW IF EXISTS " + viewName);
            env.executeTrinoUpdateAs(ALICE, format("CREATE VIEW %s AS SELECT * FROM %s", viewName, tableName));

            // Bob cannot drop Alice's view
            assertThatThrownBy(() -> env.executeTrinoUpdateAs(BOB, format("DROP VIEW %s", viewName)))
                    .hasMessageContaining("Access Denied: Cannot drop view default.%s", viewName);

            // Alice can drop her own view
            env.executeTrinoUpdateAs(ALICE, format("DROP VIEW %s", viewName));
            assertThatThrownBy(() -> env.executeTrinoAs(ALICE, format("SELECT * FROM %s", viewName)))
                    .hasMessageContaining("does not exist");
        }
        finally {
            env.executeTrinoUpdateAs(ALICE, "DROP VIEW IF EXISTS " + viewName);
            env.executeTrinoUpdateAs(ALICE, format("DROP TABLE IF EXISTS %s", tableName));
        }
    }

    @Test
    void testAccessControlSelectWithCaseSensitiveUserName(HiveKerberosImpersonationEnvironment env)
    {
        String tableName = TABLE_NAME + "_case_sensitive";
        String viewName = VIEW_NAME + "_case_sensitive";
        try {
            // Setup: Alice creates table and view
            env.executeTrinoUpdateAs(ALICE, format("DROP TABLE IF EXISTS %s", tableName));
            env.executeTrinoUpdateAs(ALICE, format("CREATE TABLE %s(month bigint, day bigint) WITH (partitioned_by = ARRAY['day'])", tableName));
            env.executeTrinoUpdateAs(ALICE, format("DROP VIEW IF EXISTS %s", viewName));
            env.executeTrinoUpdateAs(ALICE, format("CREATE VIEW %s AS SELECT month, day FROM %s", viewName, tableName));

            // CaseSensitiveUserName cannot select
            assertThatThrownBy(() -> env.executeTrinoAs(CASE_SENSITIVE_USER, format("SELECT * FROM %s", tableName)))
                    .hasMessageContaining("Access Denied: Cannot select from table default.%s", tableName);

            // Grant via Hive with SET ROLE admin
            env.executeHiveUpdate("SET ROLE admin");

            // Grant to wrong case - should still fail
            env.executeHiveUpdate(format("GRANT SELECT ON TABLE %s TO USER casesensitiveusername", tableName));
            assertThatThrownBy(() -> env.executeTrinoAs(CASE_SENSITIVE_USER, format("SELECT * FROM %s", tableName)))
                    .hasMessageContaining("Access Denied: Cannot select from table default.%s", tableName);

            // Grant with correct case - should succeed
            env.executeHiveUpdate(format("GRANT SELECT ON TABLE %s TO USER CaseSensitiveUserName", tableName));
            assertThat(env.executeTrinoAs(CASE_SENSITIVE_USER, format("SELECT * FROM %s", tableName))).hasNoRows();

            // View access
            assertThatThrownBy(() -> env.executeTrinoAs(CASE_SENSITIVE_USER, format("SELECT * FROM %s", viewName)))
                    .hasMessageContaining("Access Denied: Cannot select from table default.%s", viewName);
            env.executeHiveUpdate(format("GRANT SELECT ON TABLE %s TO USER CaseSensitiveUserName", viewName));
            assertThat(env.executeTrinoAs(CASE_SENSITIVE_USER, format("SELECT * FROM %s", viewName))).hasNoRows();
        }
        finally {
            env.executeTrinoUpdateAs(ALICE, format("DROP VIEW IF EXISTS %s", viewName));
            env.executeTrinoUpdateAs(ALICE, format("DROP TABLE IF EXISTS %s", tableName));
        }
    }

    @Test
    void testAccessControlSetTableAuthorization(HiveKerberosImpersonationEnvironment env)
    {
        // Using custom table name as drop table as different user may leave some data in HDFS
        String tableName = "set_table_authorization_test";
        try {
            env.executeTrinoUpdateAs(ALICE, format("DROP TABLE IF EXISTS %s", tableName));
            env.executeTrinoUpdateAs(ALICE, format("CREATE TABLE %s(col1 bigint, col2 bigint)", tableName));

            // Bob cannot set authorization on Alice's table
            assertThatThrownBy(() -> env.executeTrinoUpdateAs(BOB, format("ALTER TABLE %s SET AUTHORIZATION bob", tableName)))
                    .hasMessageContaining("Access Denied: Cannot set authorization for table default.%s to USER bob", tableName);

            // Admin (hdfs) can set authorization
            env.executeTrinoInSession(HDFS, session -> {
                session.executeUpdate("SET SESSION legacy_catalog_roles=true");
                session.executeUpdate("SET ROLE ADMIN");
                session.executeUpdate(format("ALTER TABLE %s SET AUTHORIZATION bob", tableName));
            });

            // Bob can now drop the table
            env.executeTrinoUpdateAs(BOB, "DROP TABLE " + tableName);
        }
        catch (Exception e) {
            // Try cleanup as alice if bob drop fails
            try {
                env.executeTrinoUpdateAs(ALICE, "DROP TABLE IF EXISTS " + tableName);
            }
            catch (Exception ignored) {
                // Ignore cleanup errors
            }
            throw e;
        }
    }

    @Test
    void testAccessControlSetViewAuthorization(HiveKerberosImpersonationEnvironment env)
    {
        String tableName = TABLE_NAME + "_view_auth_source";
        String viewName = VIEW_NAME + "_auth";
        try {
            // Setup: Alice creates table and view
            env.executeTrinoUpdateAs(ALICE, format("DROP TABLE IF EXISTS %s", tableName));
            env.executeTrinoUpdateAs(ALICE, format("CREATE TABLE %s(month bigint, day bigint) WITH (partitioned_by = ARRAY['day'])", tableName));
            env.executeTrinoUpdateAs(ALICE, format("DROP VIEW IF EXISTS %s", viewName));
            env.executeTrinoUpdateAs(ALICE, format("CREATE VIEW %s AS SELECT month, day FROM %s", viewName, tableName));

            // Bob cannot set authorization on Alice's view
            assertThatThrownBy(() -> env.executeTrinoUpdateAs(BOB, format("ALTER VIEW %s SET AUTHORIZATION bob", viewName)))
                    .hasMessageContaining("Access Denied: Cannot set authorization for view default.%s to USER bob", viewName);
            assertThatThrownBy(() -> env.executeTrinoUpdateAs(BOB, format("DROP VIEW %s", viewName)))
                    .hasMessageContaining("Access Denied: Cannot drop view default.%s", viewName);

            // Admin (hdfs) can set authorization
            env.executeTrinoInSession(HDFS, session -> {
                session.executeUpdate("SET SESSION legacy_catalog_roles=true");
                session.executeUpdate("SET ROLE ADMIN");
                session.executeUpdate(format("ALTER VIEW %s SET AUTHORIZATION bob", viewName));
            });

            // Bob can now drop the view
            env.executeTrinoUpdateAs(BOB, format("DROP VIEW %s", viewName));
        }
        finally {
            env.executeTrinoUpdateAs(BOB, "DROP VIEW IF EXISTS " + viewName);
            env.executeTrinoUpdateAs(ALICE, "DROP VIEW IF EXISTS " + viewName);
            env.executeTrinoUpdateAs(ALICE, format("DROP TABLE IF EXISTS %s", tableName));
        }
    }

    @Test
    void testAccessControlSetHiveViewAuthorization(HiveKerberosImpersonationEnvironment env)
    {
        String tableName = "test_hive_table";
        String viewName = "test_hive_view";
        try {
            // Setup: Create table and view via Hive
            env.executeHiveUpdate("DROP VIEW IF EXISTS " + viewName);
            env.executeHiveUpdate("DROP TABLE IF EXISTS " + tableName);
            env.executeHiveUpdate("CREATE TABLE " + tableName + " (col1 int)");
            env.executeHiveUpdate("CREATE VIEW " + viewName + " AS SELECT * FROM " + tableName);

            // Bob cannot set authorization
            assertThatThrownBy(() -> env.executeTrinoUpdateAs(BOB, "ALTER VIEW " + viewName + " SET AUTHORIZATION bob"))
                    .hasMessageContaining("Access Denied: Cannot set authorization for view default." + viewName + " to USER bob");
            assertThatThrownBy(() -> env.executeTrinoUpdateAs(BOB, "DROP VIEW " + viewName))
                    .hasMessageContaining("Access Denied: Cannot drop view default." + viewName);

            // Admin (hdfs) can set authorization
            env.executeTrinoInSession(HDFS, session -> {
                session.executeUpdate("SET SESSION legacy_catalog_roles=true");
                session.executeUpdate("SET ROLE ADMIN");
                session.executeUpdate("ALTER VIEW " + viewName + " SET AUTHORIZATION bob");
            });

            // Bob can now drop the view
            env.executeTrinoUpdateAs(BOB, "DROP VIEW " + viewName);
        }
        finally {
            env.executeHiveUpdate("DROP VIEW IF EXISTS " + viewName);
            env.executeHiveUpdate("DROP TABLE IF EXISTS " + tableName);
        }
    }

    @Test
    void testAccessControlShowColumns(HiveKerberosImpersonationEnvironment env)
    {
        String tableName = TABLE_NAME + "_show_columns";
        try {
            // Setup: Alice creates table
            env.executeTrinoUpdateAs(ALICE, format("DROP TABLE IF EXISTS %s", tableName));
            env.executeTrinoUpdateAs(ALICE, format("CREATE TABLE %s(month bigint, day bigint) WITH (partitioned_by = ARRAY['day'])", tableName));

            // Bob cannot show columns
            assertThatThrownBy(() -> env.executeTrinoAs(BOB, format("SHOW COLUMNS FROM %s", tableName)))
                    .hasMessageContaining("Access Denied: Cannot show columns of table default.%s", tableName);

            // Alice grants SELECT to Bob
            env.executeTrinoUpdateAs(ALICE, format("GRANT SELECT ON %s TO bob", tableName));
            assertThat(env.executeTrinoAs(BOB, format("SHOW COLUMNS FROM %s", tableName))).hasRowsCount(2);
        }
        finally {
            env.executeTrinoUpdateAs(ALICE, format("DROP TABLE IF EXISTS %s", tableName));
        }
    }

    @Test
    void testAccessControlShowStatsFor(HiveKerberosImpersonationEnvironment env)
    {
        String tableName = TABLE_NAME + "_show_stats";
        try {
            // Setup: Alice creates table
            env.executeTrinoUpdateAs(ALICE, format("DROP TABLE IF EXISTS %s", tableName));
            env.executeTrinoUpdateAs(ALICE, format("CREATE TABLE %s(month bigint, day bigint) WITH (partitioned_by = ARRAY['day'])", tableName));

            // Bob cannot show stats
            assertThatThrownBy(() -> env.executeTrinoAs(BOB, format("SHOW STATS FOR %s", tableName)))
                    .hasMessageContaining("Access Denied: Cannot select from table default.%s", tableName);

            // Alice grants SELECT to Bob
            env.executeTrinoUpdateAs(ALICE, format("GRANT SELECT ON %s TO bob", tableName));
            assertThat(env.executeTrinoAs(BOB, format("SHOW STATS FOR %s", tableName))).hasRowsCount(3);
        }
        finally {
            env.executeTrinoUpdateAs(ALICE, format("DROP TABLE IF EXISTS %s", tableName));
        }
    }

    @Test
    void testAccessControlFilterColumns(HiveKerberosImpersonationEnvironment env)
    {
        String tableName = TABLE_NAME + "_filter_columns";
        try {
            // Setup: Alice creates table
            env.executeTrinoUpdateAs(ALICE, format("DROP TABLE IF EXISTS %s", tableName));
            env.executeTrinoUpdateAs(ALICE, format("CREATE TABLE %s(month bigint, day bigint) WITH (partitioned_by = ARRAY['day'])", tableName));

            // Bob sees no columns in information_schema
            assertThat(env.executeTrinoAs(BOB, format("SELECT * FROM information_schema.columns WHERE table_name = '%s'", tableName))).hasNoRows();

            // Alice grants SELECT to Bob
            env.executeTrinoUpdateAs(ALICE, format("GRANT SELECT ON %s TO bob", tableName));
            assertThat(env.executeTrinoAs(BOB, format("SELECT * FROM information_schema.columns WHERE table_name = '%s'", tableName))).hasRowsCount(2);
        }
        finally {
            env.executeTrinoUpdateAs(ALICE, format("DROP TABLE IF EXISTS %s", tableName));
        }
    }
}
