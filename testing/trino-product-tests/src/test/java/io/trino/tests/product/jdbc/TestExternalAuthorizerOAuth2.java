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
package io.trino.tests.product.jdbc;

import io.trino.jdbc.TestingRedirectHandlerInjector;
import io.trino.testing.containers.environment.ProductTest;
import io.trino.testing.containers.environment.QueryResult;
import io.trino.testing.containers.environment.RequiresEnvironment;
import io.trino.testing.containers.environment.TpchTableResults;
import io.trino.tests.product.TestGroup;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Isolated;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import static io.trino.testing.containers.environment.QueryResultAssert.assertThat;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * OAuth2 authentication tests using JDBC.
 * <p>
 * Tests basic OAuth2 authentication and re-authentication after token expiry.
 * This tests the flow without refresh tokens enabled.
 * <p>
 * Migrated from the Tempto-based TestExternalAuthorizerOAuth2 to JUnit 5.
 */
@ProductTest
@RequiresEnvironment(JdbcOAuth2BasicEnvironment.class)
@TestGroup.Oauth2
@TestGroup.ProfileSpecificTests
@Isolated  // These tests use TestingRedirectHandlerInjector which sets global state, so they cannot run in parallel
class TestExternalAuthorizerOAuth2
{
    @Test
    void shouldAuthenticateAndExecuteQuery(JdbcOAuth2BasicEnvironment env)
            throws SQLException
    {
        TestingRedirectHandlerInjector.setRedirectHandler(env.createRedirectHandler());

        try (Connection connection = env.createTrinoConnection();
                PreparedStatement statement = connection.prepareStatement("SELECT * FROM tpch.tiny.nation");
                ResultSet results = statement.executeQuery()) {
            QueryResult result = QueryResult.forResultSet(results);
            assertThat(result).containsOnly(TpchTableResults.NATION_ROWS);
        }
    }

    @Test
    void shouldAuthenticateAfterTokenExpires(JdbcOAuth2BasicEnvironment env)
            throws SQLException, InterruptedException
    {
        TestingRedirectHandlerInjector.setRedirectHandler(env.createRedirectHandler());

        try (Connection connection = env.createTrinoConnection();
                PreparedStatement statement = connection.prepareStatement("SELECT * FROM tpch.tiny.nation");
                ResultSet results = statement.executeQuery()) {
            QueryResult result = QueryResult.forResultSet(results);
            assertThat(result).containsOnly(TpchTableResults.NATION_ROWS);

            // Wait until the token expires. See: JdbcOAuth2Environment.TTL_ACCESS_TOKEN_IN_SECONDS
            SECONDS.sleep(10);

            try (PreparedStatement repeatedStatement = connection.prepareStatement("SELECT * FROM tpch.tiny.nation");
                    ResultSet repeatedResults = repeatedStatement.executeQuery()) {
                QueryResult repeatedResult = QueryResult.forResultSet(repeatedResults);
                assertThat(repeatedResult).containsOnly(TpchTableResults.NATION_ROWS);
            }
        }
    }
}
