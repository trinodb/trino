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
import static io.trino.testing.TestingAccessControlManager.TestingPrivilegeType.SELECT_COLUMN;
import static io.trino.testing.TestingAccessControlManager.privilege;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Test(singleThreaded = true)
public class TestFilterInaccessibleColumns
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

        // Modify the filter to remove the comment, validate that comment is no longer present in the result
        accessControl.reset();
        accessControl.deny(privilege("nation.comment", SELECT_COLUMN));
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
        accessControl.deny(privilege("nation.comment", SELECT_COLUMN));
        assertThat(assertions.query("SELECT nationkey, name, regionkey FROM nation WHERE name = 'FRANCE'"))
                .matches("VALUES (BIGINT '6', CAST('FRANCE' AS VARCHAR(25)), BIGINT '3')");

        // Select all columns explicitly
        accessControl.reset();
        accessControl.deny(privilege("nation.comment", SELECT_COLUMN));
        assertThatThrownBy(() -> assertions.query("SELECT nationkey, name, regionkey, comment FROM nation WHERE name = 'FRANCE'"))
                .hasMessage("line 1:36: Column 'comment' cannot be resolved");
    }

    @Test
    public void testPredicateOnInaccessibleColumn()
    {
        // Hide name but use it in the query predicate
        accessControl.reset();
        accessControl.deny(privilege("nation.name", SELECT_COLUMN));
        assertThatThrownBy(() -> assertions.query("SELECT * FROM nation WHERE name = 'FRANCE'"))
                .hasMessage("line 1:28: Column 'name' cannot be resolved");
    }

    /**
     * Test that filtering on INFORMATION_SCHEMA tables works as expected.
     */
    @Test
    public void testFilterInformationSchema()
    {
        accessControl.reset();
        accessControl.deny(privilege("columns.table_catalog", SELECT_COLUMN));
        accessControl.deny(privilege("columns.ordinal_position", SELECT_COLUMN));
        accessControl.deny(privilege("columns.column_default", SELECT_COLUMN));
        accessControl.deny(privilege("columns.is_nullable", SELECT_COLUMN));
        accessControl.deny(privilege("columns.data_type", SELECT_COLUMN));
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
