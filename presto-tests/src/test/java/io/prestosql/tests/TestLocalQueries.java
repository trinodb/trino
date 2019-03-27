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
package io.prestosql.tests;

import com.google.common.collect.ImmutableMap;
import io.prestosql.Session;
import io.prestosql.connector.CatalogName;
import io.prestosql.metadata.SessionPropertyManager;
import io.prestosql.plugin.tpch.TpchConnectorFactory;
import io.prestosql.testing.LocalQueryRunner;
import io.prestosql.testing.MaterializedResult;
import org.testng.annotations.Test;

import static io.prestosql.SystemSessionProperties.PUSH_PARTIAL_AGGREGATION_THROUGH_JOIN;
import static io.prestosql.plugin.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static io.prestosql.testing.MaterializedResult.resultBuilder;
import static io.prestosql.testing.TestingSession.TESTING_CATALOG;
import static io.prestosql.testing.TestingSession.testSessionBuilder;
import static io.prestosql.testing.assertions.Assert.assertEquals;

public class TestLocalQueries
        extends AbstractTestQueries
{
    public TestLocalQueries()
    {
        super(TestLocalQueries::createLocalQueryRunner);
    }

    public static LocalQueryRunner createLocalQueryRunner()
    {
        Session defaultSession = testSessionBuilder()
                .setCatalog("local")
                .setSchema(TINY_SCHEMA_NAME)
                .setSystemProperty(PUSH_PARTIAL_AGGREGATION_THROUGH_JOIN, "true")
                .build();

        LocalQueryRunner localQueryRunner = new LocalQueryRunner(defaultSession);

        // add the tpch catalog
        // local queries run directly against the generator
        localQueryRunner.createCatalog(
                defaultSession.getCatalog().get(),
                new TpchConnectorFactory(1),
                ImmutableMap.of());

        localQueryRunner.getMetadata().addFunctions(CUSTOM_FUNCTIONS);

        SessionPropertyManager sessionPropertyManager = localQueryRunner.getMetadata().getSessionPropertyManager();
        sessionPropertyManager.addSystemSessionProperties(TEST_SYSTEM_PROPERTIES);
        sessionPropertyManager.addConnectorSessionProperties(new CatalogName(TESTING_CATALOG), TEST_CATALOG_PROPERTIES);

        return localQueryRunner;
    }

    @Test
    public void testShowColumnStats()
    {
        // FIXME Add tests for more complex scenario with more stats
        MaterializedResult result = computeActual("SHOW STATS FOR nation");

        MaterializedResult expectedStatistics =
                resultBuilder(getSession(), VARCHAR, DOUBLE, DOUBLE, DOUBLE, DOUBLE, VARCHAR, VARCHAR)
                        .row("nationkey", null, 25.0, 0.0, null, "0", "24")
                        .row("name", 177.0, 25.0, 0.0, null, null, null)
                        .row("regionkey", null, 5.0, 0.0, null, "0", "4")
                        .row("comment", 1857.0, 25.0, 0.0, null, null, null)
                        .row(null, null, null, null, 25.0, null, null)
                        .build();

        assertEquals(result, expectedStatistics);
    }

    @Test
    public void testRejectStarQueryWithoutFromRelation()
    {
        assertQueryFails("SELECT *", "line \\S+ SELECT \\* not allowed in queries without FROM clause");
        assertQueryFails("SELECT 1, '2', *", "line \\S+ SELECT \\* not allowed in queries without FROM clause");
    }

    @Test
    public void testDecimal()
    {
        assertQuery("SELECT DECIMAL '1.0'", "SELECT CAST('1.0' AS DECIMAL)");
        assertQuery("SELECT DECIMAL '1.'", "SELECT CAST('1.0' AS DECIMAL)");
        assertQuery("SELECT DECIMAL '0.1'", "SELECT CAST('0.1' AS DECIMAL)");
        assertQuery("SELECT 1.0", "SELECT CAST('1.0' AS DECIMAL)");
        assertQuery("SELECT 1.", "SELECT CAST('1.0' AS DECIMAL)");
        assertQuery("SELECT 0.1", "SELECT CAST('0.1' AS DECIMAL)");
    }

    @Test
    public void testHueQueries()
    {
        // https://github.com/cloudera/hue/blob/b49e98c1250c502be596667ce1f0fe118983b432/desktop/libs/notebook/src/notebook/connectors/jdbc.py#L205
        assertQuerySucceeds(getSession(), "SELECT table_name, table_comment FROM information_schema.tables WHERE table_schema='nation'");

        // https://github.com/cloudera/hue/blob/b49e98c1250c502be596667ce1f0fe118983b432/desktop/libs/notebook/src/notebook/connectors/jdbc.py#L213
        assertQuerySucceeds(getSession(), "SELECT column_name, data_type, column_comment FROM information_schema.columns WHERE table_schema='local' AND TABLE_NAME='nation'");
    }
}
