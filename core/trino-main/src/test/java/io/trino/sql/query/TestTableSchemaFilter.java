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
package io.trino.sql.query;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import io.trino.Session;
import io.trino.metadata.QualifiedObjectName;
import io.trino.plugin.tpch.TpchConnectorFactory;
import io.trino.spi.security.Identity;
import io.trino.testing.LocalQueryRunner;
import io.trino.testing.TestingAccessControlManager;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static io.trino.plugin.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Test(singleThreaded = true)
public class TestTableSchemaFilter
{
    private static final String CATALOG = "local";
    private static final String USER = "user";

    private static final Session SESSION = testSessionBuilder()
            .setCatalog(CATALOG)
            .setSchema(TINY_SCHEMA_NAME)
            .setIdentity(Identity.forUser(USER).build())
            .build();

    private QueryAssertions assertions;
    private TestingAccessControlManager accessControl;

    @BeforeClass
    public void init()
    {
        LocalQueryRunner runner = LocalQueryRunner.builder(SESSION).build();
        runner.createCatalog(CATALOG, new TpchConnectorFactory(1), ImmutableMap.of());
        assertions = new QueryAssertions(runner);
        accessControl = assertions.getQueryRunner().getAccessControl();
    }

    @AfterClass(alwaysRun = true)
    public void teardown()
    {
        assertions.close();
        assertions = null;
    }

    @Test
    public void testSimpleTableSchemaFilter()
    {
        // No filtering baseline
        accessControl.reset();
        assertThat(assertions.query("SELECT * FROM nation WHERE name = 'FRANCE'"))
                .matches("VALUES (BIGINT '6', CAST('FRANCE' AS VARCHAR(25)), BIGINT '3', CAST('refully final requests. regular, ironi' AS VARCHAR(152)))");

        // Add a filter but have it return all columns, the result should not change.
        accessControl.reset();
        accessControl.setTableSchemaFilter(
                new QualifiedObjectName(CATALOG, "tiny", "nation"),
                USER,
                Sets.newHashSet("nationkey", "name", "regionkey", "comment"));
        assertThat(assertions.query("SELECT * FROM nation WHERE name = 'FRANCE'"))
                .matches("VALUES (BIGINT '6', CAST('FRANCE' AS VARCHAR(25)), BIGINT '3', CAST('refully final requests. regular, ironi' AS VARCHAR(152)))");

        // Modify the filter to remove the comment, validate that comment is no longer present in the result
        accessControl.reset();
        accessControl.setTableSchemaFilter(
                new QualifiedObjectName(CATALOG, "tiny", "nation"),
                USER,
                Sets.newHashSet("nationkey", "name", "regionkey"));
        assertThat(assertions.query("SELECT * FROM nation WHERE name = 'FRANCE'"))
                .matches("VALUES (BIGINT '6', CAST('FRANCE' AS VARCHAR(25)), BIGINT '3')");
    }

    /**
     * Test filtering when columns are explicitly specified in SELECT
     */
    @Test
    public void testFilterExplicitSelect()
    {
        // Select the columns that are available to us explicitly
        accessControl.reset();
        accessControl.setTableSchemaFilter(
                new QualifiedObjectName(CATALOG, "tiny", "nation"),
                USER,
                Sets.newHashSet("nationkey", "name", "regionkey"));
        assertThat(assertions.query("SELECT nationkey, name, regionkey FROM nation WHERE name = 'FRANCE'"))
                .matches("VALUES (BIGINT '6', CAST('FRANCE' AS VARCHAR(25)), BIGINT '3')");

        // Select all columns explicitly
        accessControl.reset();
        accessControl.setTableSchemaFilter(
                new QualifiedObjectName(CATALOG, "tiny", "nation"),
                USER,
                Sets.newHashSet("nationkey", "name", "regionkey"));
        assertThatThrownBy(() -> assertions.query("SELECT nationkey, name, regionkey, comment FROM nation WHERE name = 'FRANCE'"))
                .hasMessage("line 1:36: Column 'comment' cannot be resolved");
    }

    /**
     * Test filtering when a hidden column is present in the predicate
     */
    @Test
    public void testFilterSelectInPredicate()
    {
        // Hide name but use it in the query predicate
        accessControl.reset();
        accessControl.setTableSchemaFilter(
                new QualifiedObjectName(CATALOG, "tiny", "nation"),
                USER,
                Sets.newHashSet("nationkey", "comment", "regionkey"));
        assertThatThrownBy(() -> assertions.query("SELECT * FROM nation WHERE name = 'FRANCE'"))
                .hasMessage("line 1:28: Column 'name' cannot be resolved");
    }

    /**
     * Test filtering when the access control implementation returns a column not part of the table
     */
    @Test
    public void testFilterUnknownColumn()
    {
        accessControl.reset();
        accessControl.setTableSchemaFilter(
                new QualifiedObjectName(CATALOG, "tiny", "nation"),
                USER,
                Sets.newHashSet("nationkey", "name", "regionkey", "comment", "orderkey"));
        assertThat(assertions.query("SELECT * FROM nation WHERE name = 'FRANCE'"))
                .matches("VALUES (BIGINT '6', CAST('FRANCE' AS VARCHAR(25)), BIGINT '3', CAST('refully final requests. regular, ironi' AS VARCHAR(152)))");
    }

    /**
     * Test that filtering out "name" from the table doesn't change SHOW COLUMNS, DESCRIBE, or SELECT on INFORMATION_SCHEMA output.
     */
    @Test
    public void testShowColumnsDescribe()
    {
        accessControl.reset();
        accessControl.setTableSchemaFilter(
                new QualifiedObjectName(CATALOG, "tiny", "nation"),
                USER,
                Sets.newHashSet("nationkey", "regionkey", "comment"));
        assertThat(assertions.query("SHOW COLUMNS FROM nation")).skippingTypesCheck().matches(
                "VALUES ('nationkey', 'bigint', '', '')," +
                        "('name', 'varchar(25)', '', '')," +
                        "('regionkey', 'bigint', '', '')," +
                        "('comment', 'varchar(152)', '', '')");
        assertThat(assertions.query("DESCRIBE nation")).skippingTypesCheck().matches(
                "VALUES ('nationkey', 'bigint', '', '')," +
                        "('name', 'varchar(25)', '', '')," +
                        "('regionkey', 'bigint', '', '')," +
                        "('comment', 'varchar(152)', '', '')");
        assertThat(assertions.query("SELECT column_name FROM information_schema.columns WHERE table_name = 'nation' AND table_schema = 'tiny'"))
                .skippingTypesCheck().matches(
                    "VALUES ('nationkey')," +
                            "('regionkey')," +
                            "('comment')," +
                            "('name')");
    }

    /**
     * Test that filtering on INFORMATION_SCHEMA tables works as expected.
     */
    @Test
    public void testFilterInformationSchema()
    {
        accessControl.reset();
        accessControl.setTableSchemaFilter(
                new QualifiedObjectName(CATALOG, "information_schema", "columns"),
                USER,
                Sets.newHashSet("column_name", "table_name", "table_schema"));
        // SHOW COLUMNS and DESCRIBE fail since data_type isn't visible
        assertThatThrownBy(() -> assertions.query("SHOW COLUMNS FROM nation")).hasMessage("Column 'data_type' cannot be resolved");
        assertThatThrownBy(() -> assertions.query("DESCRIBE nation")).hasMessage("Column 'data_type' cannot be resolved");
        // SELECT on INFORMATION_SCHEMA should provide just the visible columns
        assertThat(assertions.query("SELECT * FROM information_schema.columns WHERE table_name = 'nation' AND table_schema = 'tiny'"))
                .skippingTypesCheck().matches(
                    "VALUES ('tiny', 'nation', 'nationkey')," +
                            "('tiny', 'nation', 'regionkey')," +
                            "('tiny', 'nation', 'comment')," +
                            "('tiny', 'nation', 'name')");
    }
}
