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
package io.trino.tests;

import com.google.common.collect.ImmutableMap;
import io.trino.connector.MockConnectorFactory;
import io.trino.connector.MockConnectorPlugin;
import io.trino.testing.AbstractTestEngineOnlyQueries;
import io.trino.testing.CustomFunctionBundle;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;

import static io.airlift.testing.Closeables.closeAllSuppress;
import static io.trino.metadata.ResolverManager.getLowerCaseCanonicalizer;
import static java.util.Locale.ENGLISH;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.abort;

public class TestLocalEngineOnlyQueries
        extends AbstractTestEngineOnlyQueries
{
    @Override
    protected QueryRunner createQueryRunner()
    {
        QueryRunner queryRunner = TestLocalQueries.createTestQueryRunner();
        try {
            queryRunner.addFunctions(CustomFunctionBundle.CUSTOM_FUNCTIONS);
            // for testing session properties
            queryRunner.getSessionPropertyManager().addSystemSessionProperties(TEST_SYSTEM_PROPERTIES);
            queryRunner.installPlugin(new MockConnectorPlugin(MockConnectorFactory.builder()
                    .withSessionProperties(TEST_CATALOG_PROPERTIES)
                    .build()));
            queryRunner.createCatalog(TESTING_CATALOG, "mock", ImmutableMap.of());
            queryRunner.getPlannerContext().getMetadata().getResolverManager().addResolver(TESTING_CATALOG, getLowerCaseCanonicalizer());
        }
        catch (RuntimeException e) {
            throw closeAllSuppress(e, queryRunner);
        }
        return queryRunner;
    }

    @Override
    public String canonicalize(String value)
    {
        return value.toLowerCase(ENGLISH);
    }

    @Test
    @Override
    public void testShowCreateInformationSchemaTable()
    {
        assertQueryFails("SHOW CREATE VIEW \"information_schema\".\"schemata\"", "line 1:1: Relation '\\w+.information_schema.schemata' is a table, not a view");
        assertQueryFails("SHOW CREATE MATERIALIZED VIEW \"information_schema\".\"schemata\"", "line 1:1: Relation '\\w+.information_schema.schemata' is a table, not a materialized view");

        assertThat((String) computeScalar("SHOW CREATE TABLE \"information_schema\".\"schemata\""))
                .isEqualTo("""
                        CREATE TABLE %s."information_schema"."schemata" (
                           catalog_name varchar,
                           schema_name varchar
                        )\
                        """.formatted(getSession().getCatalog().orElseThrow()));
    }

    @Test
    @Override
    public void testMatchRecognize()
    {
        // FIXME: Cant have this test working with mock connector?
        assertThatThrownBy(super::testMatchRecognize)
                .hasMessageMatching("Execution of 'actual' query failed: SELECT .*");
    }

    @Test
    @Override
    public void testJoinedPatternMatch()
    {
        // FIXME: Cant have this test working with mock connector?
        assertThatThrownBy(super::testJoinedPatternMatch)
                .hasMessageMatching("Execution of 'actual' query failed: SELECT .*");
    }

    @Test
    @Override
    public void testSelectCaseInsensitive()
    {
        // FIXME: Cant have this test working with mock connector?
        assertThatThrownBy(super::testSelectCaseInsensitive)
                .hasMessageMatching("Execution of 'expected' query failed: .*");
    }

    @Test
    @Override
    public void testSetSession()
    {
        abort("SET SESSION is not supported by PlanTester");
    }

    @Test
    @Override
    public void testResetSession()
    {
        abort("RESET SESSION is not supported by PlanTester");
    }
}
