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
import io.airlift.slice.Slice;
import io.trino.FullConnectorSession;
import io.trino.connector.MockConnectorFactory;
import io.trino.connector.MockConnectorPlugin;
import io.trino.metadata.InternalFunctionBundle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.function.Description;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.StandardTypes;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.session.PropertyMetadata.stringProperty;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestSessionSpecifications
{
    private final QueryAssertions assertions = new QueryAssertions();

    @AfterAll
    public void teardown()
    {
        assertions.close();
    }

    @BeforeAll
    public void registerFunctions()
    {
        assertions.addFunctions(InternalFunctionBundle.builder()
                .scalars(TestSessionSpecifications.class)
                .build());
        assertions.addPlugin(new MockConnectorPlugin(MockConnectorFactory.builder()
                .withSessionProperty(stringProperty("test_property", "Test property", "", false))
                .build()));
        assertions.getQueryRunner().createCatalog("mock", "mock", ImmutableMap.of());
    }

    @Test
    public void testSetInvalidSessionProperty()
    {
            assertions.query("""
                WITH SESSION unknown_session_property = '0B'
                SELECT 1
            """)
            .assertThat()
            .failure()
            .hasMessage("Unknown session property unknown_session_property");

        assertions.query("""
                WITH SESSION query_max_memory_per_node = '10M'
                SELECT 1
            """)
                .assertThat()
                .failure()
                .hasMessage("query_max_memory_per_node is invalid: 10M");

        assertions.query("""
                WITH SESSION catalog.query_max_memory_per_node = '10M'
                SELECT 1
            """)
                .assertThat()
                .failure()
                .hasMessage("Catalog 'catalog' not found");
    }

    @Test
    public void testSetValidSessionProperty()
    {
        assertions.query("""
                WITH SESSION query_max_memory_per_node = '10MB'
                SELECT session_property('query_max_memory_per_node')
            """)
                .assertThat()
                .matches("VALUES CAST('10MB' AS varchar)");
    }

    @Test
    public void testSetValidCatalogSessionProperty()
    {
        assertions.query("""
                WITH SESSION mock.test_property = 'Hello World'
                SELECT catalog_session_property('mock', 'test_property')
            """)
                .assertThat()
                .matches("VALUES CAST('Hello World' AS varchar)");
    }

    @ScalarFunction(value = "session_property")
    @Description("Gets session property")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice getSessionProperty(ConnectorSession session, @SqlType(StandardTypes.VARCHAR) Slice propertyName)
    {
        if (session instanceof FullConnectorSession connectorSession) {
            return utf8Slice(connectorSession.getSession().getSystemProperties().get(propertyName.toStringUtf8()));
        }
        return utf8Slice("unknown");
    }

    @ScalarFunction(value = "catalog_session_property")
    @Description("Gets catalog session property")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice getCatalogSessionProperty(ConnectorSession session, @SqlType(StandardTypes.VARCHAR) Slice catalogName, @SqlType(StandardTypes.VARCHAR) Slice propertyName)
    {
        if (session instanceof FullConnectorSession connectorSession) {
            return utf8Slice(connectorSession.getSession().getCatalogProperties(catalogName.toStringUtf8()).get(propertyName.toStringUtf8()));
        }
        return utf8Slice("unknown");
    }
}
