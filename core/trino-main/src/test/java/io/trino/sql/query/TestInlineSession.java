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
import io.trino.Session;
import io.trino.connector.MockConnectorFactory;
import io.trino.connector.MockConnectorPlugin;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.testing.QueryRunner;
import io.trino.testing.StandaloneQueryRunner;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.util.Map;

import static io.trino.plugin.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.trino.spi.StandardErrorCode.INVALID_SESSION_PROPERTY;
import static io.trino.spi.session.PropertyMetadata.stringProperty;
import static io.trino.testing.TestingHandles.TEST_CATALOG_NAME;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestInlineSession
{
    private static final String MOCK_CATALOG = "mock";

    private final QueryAssertions assertions;

    public TestInlineSession()
    {
        Session session = testSessionBuilder()
                .setCatalog(TEST_CATALOG_NAME)
                .setSchema(TINY_SCHEMA_NAME)
                .build();

        QueryRunner runner = new StandaloneQueryRunner(session);
        MockConnectorPlugin mockConnectorPlugin = new MockConnectorPlugin(MockConnectorFactory.builder()
                .withSessionProperty(stringProperty("catalog_property", "Test catalog property", "", false))
                .build());

        runner.installPlugin(mockConnectorPlugin);
        runner.createCatalog(MOCK_CATALOG, "mock", ImmutableMap.of());
        runner.installPlugin(new TpchPlugin());
        runner.createCatalog(TEST_CATALOG_NAME, "tpch", ImmutableMap.of("tpch.splits-per-node", "1"));
        runner.installPlugin(new TestInlineFunctions.TestingLanguageEnginePlugin());

        assertions = new QueryAssertions(runner);
    }

    @AfterAll
    public void teardown()
    {
        assertions.close();
    }

    @Test
    public void testInlineSession()
    {
        assertThat(assertions.query("WITH SESSION time_zone_id = 'Europe/Wonderland' SELECT current_timezone()"))
                .failure()
                .hasMessageContaining("Time zone not supported: Europe/Wonderland");

        assertThat(assertions.query("WITH SESSION time_zone_id = 'Europe/Warsaw' SELECT current_timezone()"))
                .matches("VALUES CAST('Europe/Warsaw' AS varchar)");

        Session session = assertions.sessionBuilder()
                .setSystemProperty("time_zone_id", "America/Los_Angeles")
                .build();

        assertThat(assertions.query(session, "WITH SESSION time_zone_id = 'Europe/Warsaw' SELECT current_timezone()"))
                .matches("VALUES CAST('Europe/Warsaw' AS varchar)");
    }

    @Test
    public void testInlineSessionAndSqlFunctions()
    {
        assertThat(assertions.query("""
                WITH
                  SESSION time_zone_id = 'Europe/Warsaw'
                WITH
                  FUNCTION foo() RETURNS varchar RETURN current_timezone()
                SELECT foo()
                """))
                .matches("VALUES CAST('Europe/Warsaw' AS varchar)");
    }

    @Test
    void testValidSystemSessionProperties()
    {
        assertThat(assertions.execute("WITH SESSION query_max_total_memory = '10GB' SELECT 1").getSession().getSystemProperties())
                .isEqualTo(Map.of("query_max_total_memory", "10GB"));

        assertThat(assertions.execute("WITH SESSION query_max_total_memory = CAST('10GB' AS VARCHAR) SELECT 1").getSession().getSystemProperties())
                .isEqualTo(Map.of("query_max_total_memory", "10GB"));
    }

    @Test
    void testValidCatalogSessionProperty()
    {
        assertThat(assertions.execute("WITH SESSION mock.catalog_property = 'true' SELECT 1").getSession().getCatalogProperties("mock"))
                .isEqualTo(Map.of("catalog_property", "true"));

        assertThat(assertions.execute("WITH SESSION mock.catalog_property = CAST(true AS varchar) SELECT 1").getSession().getCatalogProperties("mock"))
                .isEqualTo(Map.of("catalog_property", "true"));
    }

    @Test
    void testInvalidSessionProperty()
    {
        assertThat(assertions.query("WITH SESSION test.schema.invalid_key = 'invalid_value' SELECT 1"))
                .failure()
                .hasMessageContaining("line 1:14: Invalid session property 'test.schema.invalid_key'");
    }

    @Test
    void testInvalidSystemSessionProperties()
    {
        assertThat(assertions.query("WITH SESSION invalid_key = 'invalid_value' SELECT 1"))
                .failure()
                .hasErrorCode(INVALID_SESSION_PROPERTY)
                .hasMessageContaining("line 1:28: Session property 'invalid_key' does not exist");

        assertThat(assertions.query("WITH SESSION query_max_total_memory = 'invalid_value' SELECT 1"))
                .failure()
                .hasErrorCode(INVALID_SESSION_PROPERTY)
                .hasMessageContaining("line 1:39: size is not a valid data size string: invalid_value");

        assertThat(assertions.query("WITH SESSION query_max_total_memory = '10GB', query_max_total_memory = '16GB' SELECT 1"))
                .failure()
                .hasErrorCode(INVALID_SESSION_PROPERTY)
                .hasMessageContaining("line 1:47: Session property 'query_max_total_memory' already set");
    }

    @Test
    void testInvalidCatalogSessionProperties()
    {
        assertThat(assertions.query("WITH SESSION mock.invalid_key = 'invalid_value' SELECT 1"))
                .failure()
                .hasMessageContaining("line 1:33: Session property 'mock.invalid_key' does not exist");

        assertThat(assertions.query("WITH SESSION mock.catalog_property = true SELECT 1"))
                .failure()
                .hasMessageContaining("Unable to set session property 'mock.catalog_property' to 'true': Cannot cast type boolean to varchar");

        assertThat(assertions.query("WITH SESSION mock.catalog_property = 'true', mock.catalog_property = 'false' SELECT 1"))
                .failure()
                .hasMessageContaining("line 1:46: Session property 'mock.catalog_property' already set");
    }
}
