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
package io.trino;

import io.trino.jdbc.TestJdbcResultSet;
import io.trino.server.testing.TestingTrinoServer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Optional;
import java.util.Properties;

import static io.trino.JdbcDriverCapabilities.testedVersion;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestJdbcResultSetCompatibilityOldDriver
        extends TestJdbcResultSet
{
    private static final Optional<Integer> VERSION_UNDER_TEST = testedVersion();

    private TestingTrinoServer server;

    @Override
    @BeforeAll
    public void setupServer()
    {
        server = TestingTrinoServer.builder().build();
    }

    @Override
    @AfterAll
    public void tearDownServer()
    {
        if (server != null) {
            try {
                server.close();
            }
            catch (Exception e) {
                throw new RuntimeException(e);
            }
            finally {
                server = null;
            }
        }
    }

    @Override
    protected Connection createConnection()
            throws SQLException
    {
        Properties properties = new Properties();
        properties.setProperty("user", "test");
        return DriverManager.getConnection(format("jdbc:trino://%s", server.getAddress()), properties);
    }

    @Test
    public void ensureProperTestVersionLoaded()
    {
        Assumptions.assumeTrue(
                VERSION_UNDER_TEST.isPresent(),
                "Information about JDBC version under test is missing");

        assertThat(TestJdbcResultSet.class.getPackage().getImplementationVersion())
                .isEqualTo(VERSION_UNDER_TEST.get().toString());
    }

    @Override
    @Test
    public void testDate()
            throws Exception
    {
        try {
            super.testDate();
        }
        catch (AssertionError e) {
            // Old drivers are bundled with outdated tzdata which causes some of the tested dates to have a different representation
            assertThat(e).hasMessageContaining("1970-01-01");
        }
    }
}
