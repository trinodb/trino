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
package io.trino.tests.product.ldap;

import io.trino.testing.containers.environment.ProductTest;
import io.trino.testing.containers.environment.QueryResult;
import io.trino.testing.containers.environment.RequiresEnvironment;
import io.trino.testing.containers.environment.TpchTableResults;
import io.trino.tests.product.TestGroup;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static io.trino.testing.containers.environment.QueryResultAssert.assertThat;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * LDAP authentication tests using JDBC.
 * <p>
 * Tests basic LDAP authentication scenarios including valid login, wrong password,
 * user not in group, SSL requirements, etc.
 */
@ProductTest
@RequiresEnvironment(LdapBasicEnvironment.class)
@TestGroup.Ldap
@TestGroup.TrinoJdbc
@TestGroup.ProfileSpecificTests
class TestLdapTrinoJdbc
{
    private static final String NATION_SELECT_ALL_QUERY = "SELECT * FROM tpch.tiny.nation";

    @Test
    void shouldRunQueryWithLdap(LdapBasicEnvironment env)
            throws Exception
    {
        try (Connection conn = env.createTrinoConnection();
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(NATION_SELECT_ALL_QUERY)) {
            QueryResult result = QueryResult.forResultSet(rs);
            assertThat(result).containsOnly(TpchTableResults.NATION_ROWS);
        }
    }

    @Test
    @TestGroup.LdapMultipleBinds
    void shouldRunQueryWithAlternativeBind(LdapBasicEnvironment env)
            throws Exception
    {
        try (Connection conn = env.createTrinoConnection(env.getUserInAmerica(), env.getLdapPassword());
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(NATION_SELECT_ALL_QUERY)) {
            QueryResult result = QueryResult.forResultSet(rs);
            assertThat(result).containsOnly(TpchTableResults.NATION_ROWS);
        }
    }

    @Test
    void shouldFailQueryForLdapUserInChildGroup(LdapBasicEnvironment env)
    {
        expectQueryToFailForUserNotInGroup(env, env.getChildGroupUser());
    }

    @Test
    void shouldFailQueryForLdapUserInParentGroup(LdapBasicEnvironment env)
    {
        expectQueryToFailForUserNotInGroup(env, env.getParentGroupUser());
    }

    @Test
    void shouldFailQueryForOrphanLdapUser(LdapBasicEnvironment env)
    {
        expectQueryToFailForUserNotInGroup(env, env.getOrphanUser());
    }

    @Test
    @TestGroup.LdapMultipleBinds
    void shouldFailQueryForFailedBind(LdapBasicEnvironment env)
    {
        expectQueryToFail(env, env.getUserInEurope(), env.getLdapPassword(), env.expectedFailedBindMessage());
    }

    @Test
    void shouldFailQueryForWrongLdapPassword(LdapBasicEnvironment env)
    {
        expectQueryToFail(env, env.getDefaultUser(), "wrong_password", env.expectedWrongLdapPasswordMessage());
    }

    @Test
    void shouldFailQueryForWrongLdapUser(LdapBasicEnvironment env)
    {
        assertThatThrownBy(() -> executeLdapQuery(env, NATION_SELECT_ALL_QUERY, "invalid_user", env.getLdapPassword()))
                .isInstanceOf(SQLException.class)
                .hasMessage(env.expectedWrongLdapUserMessage());
    }

    @Test
    void shouldFailQueryForEmptyUser(LdapBasicEnvironment env)
    {
        expectQueryToFail(env, "", env.getLdapPassword(),
                "Connection property user value is empty");
    }

    @Test
    void shouldFailQueryForLdapWithoutPassword(LdapBasicEnvironment env)
    {
        expectQueryToFail(env, env.getDefaultUser(), null,
                "Authentication failed: Unauthorized");
    }

    @Test
    void shouldFailQueryForLdapWithoutSsl(LdapBasicEnvironment env)
    {
        String url = format("jdbc:trino://%s:%d", env.getTrinoHost(), env.getTrinoPort());
        assertThatThrownBy(() -> DriverManager.getConnection(url, env.getDefaultUser(), env.getLdapPassword()))
                .isInstanceOf(SQLException.class)
                .hasMessageContaining("TLS/SSL is required for authentication with username and password");
    }

    @Test
    void shouldFailForIncorrectTrustStore(LdapBasicEnvironment env)
    {
        String url = format("jdbc:trino://%s:%d?SSL=true&SSLTrustStorePath=%s&SSLTrustStorePassword=%s",
                env.getTrinoHost(), env.getTrinoPort(), env.getTruststorePath(), "wrong_password");
        assertThatThrownBy(() -> DriverManager.getConnection(url, env.getDefaultUser(), env.getLdapPassword()))
                .isInstanceOf(SQLException.class)
                .hasMessageContaining("Error setting up SSL: keystore password was incorrect");
    }

    @Test
    void shouldFailForUserWithColon(LdapBasicEnvironment env)
    {
        expectQueryToFail(env, "UserWith:Colon", env.getLdapPassword(),
                "Illegal character ':' found in username");
    }

    private void expectQueryToFailForUserNotInGroup(LdapBasicEnvironment env, String user)
    {
        expectQueryToFail(env, user, env.getLdapPassword(), env.expectedUserNotInGroupMessage(user));
    }

    private void expectQueryToFail(LdapEnvironment env, String user, String password, String message)
    {
        assertThatThrownBy(() -> executeLdapQuery(env, NATION_SELECT_ALL_QUERY, user, password))
                .isInstanceOf(SQLException.class)
                .hasMessage(message);
    }

    private ResultSet executeLdapQuery(LdapEnvironment env, String query, String user, String password)
            throws SQLException
    {
        Connection connection = env.createTrinoConnection(user, password);
        Statement statement = connection.createStatement();
        return statement.executeQuery(query);
    }
}
