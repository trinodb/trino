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
package io.trino.security;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.testing.Assertions;
import io.trino.Session;
import io.trino.plugin.blackhole.BlackHolePlugin;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.spi.security.Identity;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingSession;
import org.testng.annotations.Test;

import static io.trino.SystemSessionProperties.QUERY_MAX_MEMORY;
import static io.trino.testing.TestingAccessControlManager.TestingPrivilegeType.ADD_COLUMN;
import static io.trino.testing.TestingAccessControlManager.TestingPrivilegeType.CREATE_MATERIALIZED_VIEW;
import static io.trino.testing.TestingAccessControlManager.TestingPrivilegeType.CREATE_TABLE;
import static io.trino.testing.TestingAccessControlManager.TestingPrivilegeType.CREATE_VIEW;
import static io.trino.testing.TestingAccessControlManager.TestingPrivilegeType.CREATE_VIEW_WITH_SELECT_COLUMNS;
import static io.trino.testing.TestingAccessControlManager.TestingPrivilegeType.DELETE_TABLE;
import static io.trino.testing.TestingAccessControlManager.TestingPrivilegeType.DROP_COLUMN;
import static io.trino.testing.TestingAccessControlManager.TestingPrivilegeType.DROP_TABLE;
import static io.trino.testing.TestingAccessControlManager.TestingPrivilegeType.EXECUTE_FUNCTION;
import static io.trino.testing.TestingAccessControlManager.TestingPrivilegeType.EXECUTE_QUERY;
import static io.trino.testing.TestingAccessControlManager.TestingPrivilegeType.GRANT_EXECUTE_FUNCTION;
import static io.trino.testing.TestingAccessControlManager.TestingPrivilegeType.INSERT_TABLE;
import static io.trino.testing.TestingAccessControlManager.TestingPrivilegeType.RENAME_COLUMN;
import static io.trino.testing.TestingAccessControlManager.TestingPrivilegeType.RENAME_TABLE;
import static io.trino.testing.TestingAccessControlManager.TestingPrivilegeType.SELECT_COLUMN;
import static io.trino.testing.TestingAccessControlManager.TestingPrivilegeType.SET_SESSION;
import static io.trino.testing.TestingAccessControlManager.TestingPrivilegeType.SET_USER;
import static io.trino.testing.TestingAccessControlManager.TestingPrivilegeType.SHOW_COLUMNS;
import static io.trino.testing.TestingAccessControlManager.TestingPrivilegeType.SHOW_CREATE_TABLE;
import static io.trino.testing.TestingAccessControlManager.TestingPrivilegeType.UPDATE_TABLE;
import static io.trino.testing.TestingAccessControlManager.privilege;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static io.trino.testing.sql.TestTable.randomTableSuffix;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestAccessControl
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        Session session = testSessionBuilder()
                .setCatalog("blackhole")
                .setSchema("default")
                .build();
        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(session)
                .setNodeCount(1)
                .build();
        queryRunner.installPlugin(new BlackHolePlugin());
        queryRunner.createCatalog("blackhole", "blackhole");
        queryRunner.installPlugin(new TpchPlugin());
        queryRunner.createCatalog("tpch", "tpch");
        for (String tableName : ImmutableList.of("orders", "nation", "region", "lineitem")) {
            queryRunner.execute(format("CREATE TABLE %1$s AS SELECT * FROM tpch.tiny.%1$s WITH NO DATA", tableName));
        }
        return queryRunner;
    }

    @Test
    public void testAccessControl()
    {
        assertAccessDenied("SELECT * FROM orders", "Cannot execute query", privilege("query", EXECUTE_QUERY));
        assertAccessDenied("INSERT INTO orders SELECT * FROM orders", "Cannot insert into table .*.orders.*", privilege("orders", INSERT_TABLE));
        assertAccessDenied("DELETE FROM orders", "Cannot delete from table .*.orders.*", privilege("orders", DELETE_TABLE));
        assertAccessDenied("CREATE TABLE foo AS SELECT * FROM orders", "Cannot create table .*.foo.*", privilege("foo", CREATE_TABLE));
        assertAccessDenied("SELECT * FROM nation", "Cannot select from columns \\[nationkey, regionkey, name, comment] in table .*.nation.*", privilege("nation.nationkey", SELECT_COLUMN));
        assertAccessDenied("SELECT * FROM (SELECT * FROM nation)", "Cannot select from columns \\[nationkey, regionkey, name, comment] in table .*.nation.*", privilege("nation.nationkey", SELECT_COLUMN));
        assertAccessDenied("SELECT name FROM (SELECT * FROM nation)", "Cannot select from columns \\[nationkey, regionkey, name, comment] in table .*.nation.*", privilege("nation.nationkey", SELECT_COLUMN));
        assertAccessAllowed("SELECT name FROM nation", privilege("nationkey", SELECT_COLUMN));
        assertAccessDenied("SELECT n1.nationkey, n2.regionkey FROM nation n1, nation n2", "Cannot select from columns \\[nationkey, regionkey] in table .*.nation.*", privilege("nation.nationkey", SELECT_COLUMN));
        assertAccessDenied("SELECT count(name) as c FROM nation where comment > 'abc' GROUP BY regionkey having max(nationkey) > 10", "Cannot select from columns \\[nationkey, regionkey, name, comment] in table .*.nation.*", privilege("nation.nationkey", SELECT_COLUMN));
        assertAccessDenied("SELECT 1 FROM region, nation where region.regionkey = nation.nationkey", "Cannot select from columns \\[nationkey] in table .*.nation.*", privilege("nation.nationkey", SELECT_COLUMN));
        assertAccessDenied("SELECT count(*) FROM nation", "Cannot select from columns \\[] in table .*.nation.*", privilege("nation", SELECT_COLUMN));
        assertAccessDenied("WITH t1 AS (SELECT * FROM nation) SELECT * FROM t1", "Cannot select from columns \\[nationkey, regionkey, name, comment] in table .*.nation.*", privilege("nation.nationkey", SELECT_COLUMN));
        assertAccessAllowed("SELECT name AS my_alias FROM nation", privilege("my_alias", SELECT_COLUMN));
        assertAccessAllowed("SELECT my_alias from (SELECT name AS my_alias FROM nation)", privilege("my_alias", SELECT_COLUMN));
        assertAccessDenied("SELECT name AS my_alias FROM nation", "Cannot select from columns \\[name] in table .*.nation.*", privilege("nation.name", SELECT_COLUMN));

        assertAccessDenied(
                "SELECT orders.custkey, lineitem.quantity FROM orders JOIN lineitem USING (orderkey)",
                "Cannot select from columns \\[orderkey, custkey] in table .*",
                privilege("orders.orderkey", SELECT_COLUMN));

        assertAccessDenied(
                "SELECT orders.custkey, lineitem.quantity FROM orders JOIN lineitem USING (orderkey)",
                "Cannot select from columns \\[orderkey, quantity] in table .*",
                privilege("lineitem.orderkey", SELECT_COLUMN));

        assertAccessDenied("SHOW CREATE TABLE orders", "Cannot show create table for .*.orders.*", privilege("orders", SHOW_CREATE_TABLE));
        assertAccessAllowed("SHOW CREATE TABLE lineitem", privilege("orders", SHOW_CREATE_TABLE));
        assertAccessDenied("SELECT abs(1)", "Cannot execute function abs", privilege("abs", EXECUTE_FUNCTION));
        assertAccessAllowed("SELECT abs(1)", privilege("max", EXECUTE_FUNCTION));
        assertAccessAllowed("SHOW STATS FOR lineitem");
        assertAccessAllowed("SHOW STATS FOR lineitem", privilege("orders", SELECT_COLUMN));
        assertAccessAllowed("SHOW STATS FOR (SELECT * FROM lineitem)");
        assertAccessAllowed("SHOW STATS FOR (SELECT * FROM lineitem)", privilege("orders", SELECT_COLUMN));
        assertAccessDenied("SHOW STATS FOR (SELECT * FROM nation)", "Cannot select from columns \\[nationkey, regionkey, name, comment] in table or view .*.nation", privilege("nation", SELECT_COLUMN));
        assertAccessDenied("SHOW STATS FOR (SELECT nationkey FROM nation)", "Cannot select from columns \\[nationkey] in table or view .*.nation", privilege("nation", SELECT_COLUMN));
        assertAccessDenied("SHOW STATS FOR (SELECT nationkey FROM nation)", "Cannot select from columns \\[nationkey] in table or view .*.nation", privilege("nation.nationkey", SELECT_COLUMN));
        assertAccessDenied("SHOW STATS FOR (SELECT *, nationkey FROM nation)", "Cannot select from columns \\[nationkey, regionkey, name, comment] in table or view .*.nation", privilege("nation.nationkey", SELECT_COLUMN));
        assertAccessDenied("SHOW STATS FOR (SELECT *, * FROM nation)", "Cannot select from columns \\[nationkey, regionkey, name, comment] in table or view .*.nation", privilege("nation.nationkey", SELECT_COLUMN));
        assertAccessDenied("SHOW STATS FOR (SELECT linenumber, orderkey FROM lineitem)", "Cannot select from columns \\[linenumber, orderkey] in table or view .*.lineitem.*", privilege("lineitem", SELECT_COLUMN));
        assertAccessDenied("SHOW STATS FOR (SELECT linenumber, orderkey, quantity FROM lineitem)", "Cannot select from columns \\[linenumber, orderkey, quantity] in table or view .*.lineitem.*", privilege("lineitem.linenumber", SELECT_COLUMN), privilege("lineitem.orderkey", SELECT_COLUMN));
        assertAccessDenied("SHOW STATS FOR (SELECT nationkey FROM nation)", "Cannot select from columns \\[nationkey] in table or view .*.nation.*", privilege("nation", SELECT_COLUMN));
        assertAccessDenied("SHOW STATS FOR (SELECT * FROM nation)", "Cannot select from columns \\[nationkey, regionkey, name, comment] in table or view .*.nation.*", privilege("nation", SELECT_COLUMN));
    }

    @Test
    public void testViewColumnAccessControl()
    {
        Session viewOwnerSession = TestingSession.testSessionBuilder()
                .setIdentity(Identity.ofUser("test_view_access_owner"))
                .setCatalog(getSession().getCatalog().orElseThrow())
                .setSchema(getSession().getSchema().orElseThrow())
                .build();

        String columnAccessViewName = "test_view_column_access_" + randomTableSuffix();

        // TEST COLUMN-LEVEL PRIVILEGES
        // view creation permissions are only checked at query time, not at creation
        assertAccessAllowed(
                viewOwnerSession,
                "CREATE VIEW " + columnAccessViewName + " AS SELECT * FROM orders",
                privilege("orders", CREATE_VIEW_WITH_SELECT_COLUMNS));

        // verify selecting from a view over a table requires the view owner to have special view creation privileges for the table
        assertAccessDenied(
                "SELECT * FROM " + columnAccessViewName,
                "View owner does not have sufficient privileges: View owner 'test_view_access_owner' cannot create view that selects from \\w+.\\w+.orders\\w*",
                privilege(viewOwnerSession.getUser(), "orders", CREATE_VIEW_WITH_SELECT_COLUMNS));

        // verify the view owner can select from the view even without special view creation privileges
        assertAccessAllowed(
                viewOwnerSession,
                "SELECT * FROM " + columnAccessViewName,
                privilege(viewOwnerSession.getUser(), "orders", CREATE_VIEW_WITH_SELECT_COLUMNS));

        // verify selecting from a view over a table does not require the session user to have SELECT privileges on the underlying table
        assertAccessAllowed(
                "SELECT * FROM " + columnAccessViewName,
                privilege(getSession().getUser(), "orders", CREATE_VIEW_WITH_SELECT_COLUMNS));
        assertAccessAllowed(
                "SELECT * FROM " + columnAccessViewName,
                privilege(getSession().getUser(), "orders", SELECT_COLUMN));

        Session nestedViewOwnerSession = TestingSession.testSessionBuilder()
                .setIdentity(Identity.ofUser("test_nested_view_access_owner"))
                .setCatalog(getSession().getCatalog().get())
                .setSchema(getSession().getSchema().get())
                .build();

        String nestedViewName = "test_nested_view_column_access_" + randomTableSuffix();
        // view creation permissions are only checked at query time, not at creation
        assertAccessAllowed(
                nestedViewOwnerSession,
                "CREATE VIEW " + nestedViewName + " AS SELECT * FROM " + columnAccessViewName,
                privilege(columnAccessViewName, CREATE_VIEW_WITH_SELECT_COLUMNS));

        // verify selecting from a view over a view requires the view owner of the outer view to have special view creation privileges for the inner view
        assertAccessDenied(
                "SELECT * FROM " + nestedViewName,
                "View owner does not have sufficient privileges: View owner 'test_nested_view_access_owner' cannot create view that selects from \\w+.\\w+.test_view_column_access\\w*",
                privilege(nestedViewOwnerSession.getUser(), columnAccessViewName, CREATE_VIEW_WITH_SELECT_COLUMNS));

        // verify selecting from a view over a view does not require the session user to have SELECT privileges for the inner view
        assertAccessAllowed(
                "SELECT * FROM " + nestedViewName,
                privilege(getSession().getUser(), columnAccessViewName, CREATE_VIEW_WITH_SELECT_COLUMNS));
        assertAccessAllowed(
                "SELECT * FROM " + nestedViewName,
                privilege(getSession().getUser(), columnAccessViewName, SELECT_COLUMN));

        // verify that INVOKER security runs as session user
        String invokerViewName = "test_invoker_view_column_access_" + randomTableSuffix();
        assertAccessAllowed(
                viewOwnerSession,
                "CREATE VIEW " + invokerViewName + " SECURITY INVOKER AS SELECT * FROM orders",
                privilege("orders", CREATE_VIEW_WITH_SELECT_COLUMNS));
        assertAccessAllowed(
                "SELECT * FROM " + invokerViewName,
                privilege(viewOwnerSession.getUser(), "orders", SELECT_COLUMN));
        assertAccessDenied(
                "SELECT * FROM " + invokerViewName,
                "Cannot select from columns \\[.*] in table .*.orders.*",
                privilege(getSession().getUser(), "orders", SELECT_COLUMN));

        // verify that groups are set inside access control
        executeExclusively(() -> {
            try {
                // require view owner to be in a group to access table
                getQueryRunner().getAccessControl().denyIdentityTable((identity, table) -> identity.getGroups().contains("testgroup") || !"orders".equals(table));
                assertThatThrownBy(() -> getQueryRunner().execute(getSession(), "SELECT * FROM " + columnAccessViewName))
                        .hasMessageMatching("Access Denied: View owner does not have sufficient privileges: View owner 'test_view_access_owner' cannot create view that selects from \\w+.\\w+.orders");

                // verify view can be queried when owner is in group
                getQueryRunner().getGroupProvider().setUserGroups(ImmutableMap.of(viewOwnerSession.getUser(), ImmutableSet.of("testgroup")));
                getQueryRunner().execute(getSession(), "SELECT * FROM " + columnAccessViewName);
            }
            finally {
                getQueryRunner().getAccessControl().reset();
                getQueryRunner().getGroupProvider().reset();
            }
        });

        // change access denied exception to view
        assertAccessDenied("SHOW CREATE VIEW " + nestedViewName, "Cannot show create table for .*test_nested_view_column_access.*", privilege(nestedViewName, SHOW_CREATE_TABLE));
        assertAccessAllowed("SHOW CREATE VIEW " + nestedViewName, privilege("test_denied_access_view", SHOW_CREATE_TABLE));

        assertAccessAllowed(nestedViewOwnerSession, "DROP VIEW " + nestedViewName);
        assertAccessAllowed(viewOwnerSession, "DROP VIEW " + columnAccessViewName);
        assertAccessAllowed(viewOwnerSession, "DROP VIEW " + invokerViewName);
    }

    @Test
    public void testViewFunctionAccessControl()
    {
        Session viewOwnerSession = TestingSession.testSessionBuilder()
                .setIdentity(Identity.ofUser("test_view_access_owner"))
                .setCatalog(getSession().getCatalog().orElseThrow())
                .setSchema(getSession().getSchema().orElseThrow())
                .build();

        // TEST FUNCTION PRIVILEGES
        // view creation permissions are only checked at query time, not at creation
        String functionAccessViewName = "test_view_function_access_" + randomTableSuffix();
        assertAccessAllowed(
                viewOwnerSession,
                "CREATE VIEW " + functionAccessViewName + " AS SELECT abs(1) AS c",
                privilege("abs", GRANT_EXECUTE_FUNCTION));

        assertAccessDenied(
                "SELECT * FROM " + functionAccessViewName,
                "View owner does not have sufficient privileges: 'test_view_access_owner' cannot grant 'abs' execution to user '\\w*'",
                privilege(viewOwnerSession.getUser(), "abs", GRANT_EXECUTE_FUNCTION));

        // verify executing from a view over a function does not require the session user to have execute privileges on the underlying function
        assertAccessAllowed(
                "SELECT * FROM " + functionAccessViewName,
                privilege(getSession().getUser(), "abs", EXECUTE_FUNCTION));

        // TEST SECURITY INVOKER
        // view creation permissions are only checked at query time, not at creation
        String invokerFunctionAccessViewName = "test_invoker_view_function_access_" + randomTableSuffix();
        assertAccessAllowed(
                viewOwnerSession,
                "CREATE VIEW " + invokerFunctionAccessViewName + " SECURITY INVOKER AS SELECT abs(1) AS c",
                privilege("abs", GRANT_EXECUTE_FUNCTION));
        assertAccessAllowed(
                "SELECT * FROM " + invokerFunctionAccessViewName,
                privilege(viewOwnerSession.getUser(), "abs", EXECUTE_FUNCTION));
        assertAccessDenied(
                "SELECT * FROM " + invokerFunctionAccessViewName,
                "Cannot execute function abs",
                privilege(getSession().getUser(), "abs", EXECUTE_FUNCTION));

        assertAccessAllowed(viewOwnerSession, "DROP VIEW " + functionAccessViewName);
        assertAccessAllowed(viewOwnerSession, "DROP VIEW " + invokerFunctionAccessViewName);
    }

    @Test
    public void testAnalyzeAccessControl()
    {
        assertAccessAllowed("ANALYZE nation");
        assertAccessDenied("ANALYZE nation", "Cannot ANALYZE \\(missing insert privilege\\) table .*.nation.*", privilege("nation", INSERT_TABLE));
        assertAccessDenied("ANALYZE nation", "Cannot select from columns \\[.*] in table or view .*.nation", privilege("nation", SELECT_COLUMN));
        assertAccessDenied("ANALYZE nation", "Cannot select from columns \\[.*nationkey.*] in table or view .*.nation", privilege("nation.nationkey", SELECT_COLUMN));
    }

    @Test
    public void testDeleteAccessControl()
    {
        assertAccessDenied("DELETE FROM orders WHERE orderkey < 12", "Cannot select from columns \\[orderkey] in table or view .*." + "orders" + ".*", privilege("orders" + ".orderkey", SELECT_COLUMN));
        assertAccessAllowed("DELETE FROM orders WHERE orderkey < 12", privilege("orders" + ".orderdate", SELECT_COLUMN));
        assertAccessAllowed("DELETE FROM orders", privilege("orders", SELECT_COLUMN));
    }

    @Test
    public void testUpdateAccessControl()
    {
        assertAccessDenied("UPDATE orders SET orderkey=123", "Cannot update columns \\[orderkey] in table .*", privilege("orders", UPDATE_TABLE));
        assertThatThrownBy(() -> getQueryRunner().execute(getSession(), "UPDATE orders SET orderkey=123"))
                .hasMessageContaining("This connector does not support updates");
    }

    @Test
    public void testNonQueryAccessControl()
    {
        assertAccessDenied("SET SESSION " + QUERY_MAX_MEMORY + " = '10MB'",
                "Cannot set system session property " + QUERY_MAX_MEMORY,
                privilege(QUERY_MAX_MEMORY, SET_SESSION));

        assertAccessDenied("CREATE TABLE foo (pk bigint)", "Cannot create table .*.foo.*", privilege("foo", CREATE_TABLE));
        assertAccessDenied("DROP TABLE orders", "Cannot drop table .*.orders.*", privilege("orders", DROP_TABLE));
        assertAccessDenied("ALTER TABLE orders RENAME TO foo", "Cannot rename table .*.orders.* to .*.foo.*", privilege("orders", RENAME_TABLE));
        assertAccessDenied("ALTER TABLE orders ADD COLUMN foo bigint", "Cannot add a column to table .*.orders.*", privilege("orders", ADD_COLUMN));
        assertAccessDenied("ALTER TABLE orders DROP COLUMN foo", "Cannot drop a column from table .*.orders.*", privilege("orders", DROP_COLUMN));
        assertAccessDenied("ALTER TABLE orders RENAME COLUMN orderkey TO foo", "Cannot rename a column in table .*.orders.*", privilege("orders", RENAME_COLUMN));
        assertAccessDenied("CREATE VIEW foo as SELECT * FROM orders", "Cannot create view .*.foo.*", privilege("foo", CREATE_VIEW));
        assertAccessDenied("CREATE MATERIALIZED VIEW foo as SELECT * FROM orders", "Cannot create materialized view .*.foo.*", privilege("foo", CREATE_MATERIALIZED_VIEW));
        // todo add DROP VIEW test... not all connectors have view support

        try {
            assertAccessDenied("SELECT 1", "Principal .* cannot become user " + getSession().getUser() + ".*", privilege(getSession().getUser(), SET_USER));
        }
        catch (AssertionError e) {
            // There is no clean exception message for authorization failure.  We simply get a 403
            Assertions.assertContains(e.getMessage(), "statusCode=403");
        }
    }

    @Test
    public void testDescribe()
    {
        assertAccessDenied("DESCRIBE orders", "Cannot show columns of table default.orders", privilege("orders", SHOW_COLUMNS));
        executeExclusively(() -> {
            try {
                getQueryRunner().getAccessControl().deny(privilege("orders.orderkey", SELECT_COLUMN));
                assertQuery(
                        "DESCRIBE orders",
                        "VALUES " +
                                // orderkey column is filtered
                                "('custkey', 'bigint', '', '')," +
                                "('orderstatus', 'varchar(1)', '', '')," +
                                "('totalprice', 'double', '', '')," +
                                "('orderdate', 'date', '', '')," +
                                "('orderpriority', 'varchar(15)', '', '')," +
                                "('clerk', 'varchar(15)', '', '')," +
                                "('shippriority', 'integer', '', '')," +
                                "('comment', 'varchar(79)', '', '')");
            }
            finally {
                getQueryRunner().getAccessControl().reset();
            }
        });
    }

    @Test
    public void testDescribeForViews()
    {
        String viewName = "describe_orders_view" + randomTableSuffix();
        assertUpdate("CREATE VIEW " + viewName + " AS SELECT * FROM orders");
        assertAccessDenied("DESCRIBE " + viewName, "Cannot show columns of table default.*", privilege(viewName, SHOW_COLUMNS));
        executeExclusively(() -> {
            try {
                getQueryRunner().getAccessControl().deny(privilege(viewName + ".orderkey", SELECT_COLUMN));
                assertQuery(
                        "DESCRIBE " + viewName,
                        "VALUES " +
                                // orderkey column is filtered
                                "('custkey', 'bigint', '', '')," +
                                "('orderstatus', 'varchar(1)', '', '')," +
                                "('totalprice', 'double', '', '')," +
                                "('orderdate', 'date', '', '')," +
                                "('orderpriority', 'varchar(15)', '', '')," +
                                "('clerk', 'varchar(15)', '', '')," +
                                "('shippriority', 'integer', '', '')," +
                                "('comment', 'varchar(79)', '', '')");
            }
            finally {
                getQueryRunner().getAccessControl().reset();
            }
        });
        assertUpdate("DROP VIEW " + viewName);
    }
}
