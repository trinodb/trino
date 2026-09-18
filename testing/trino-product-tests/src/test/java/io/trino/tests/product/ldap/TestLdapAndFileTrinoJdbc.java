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
import java.sql.ResultSet;
import java.sql.Statement;

import static io.trino.testing.containers.environment.QueryResultAssert.assertThat;

/**
 * LDAP and file-based authentication tests using JDBC.
 * <p>
 * Tests scenarios where both LDAP and file password authenticators are configured.
 */
@ProductTest
@RequiresEnvironment(LdapAndFileEnvironment.class)
@TestGroup.LdapAndFile
@TestGroup.TrinoJdbc
@TestGroup.ProfileSpecificTests
class TestLdapAndFileTrinoJdbc
{
    private static final String NATION_SELECT_ALL_QUERY = "SELECT * FROM tpch.tiny.nation";

    @Test
    void shouldRunQueryWithLdapAuthenticator(LdapAndFileEnvironment env)
            throws Exception
    {
        // Use LDAP password to authenticate
        try (Connection conn = env.createTrinoConnection(env.getDefaultUser(), env.getLdapPassword());
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(NATION_SELECT_ALL_QUERY)) {
            QueryResult result = QueryResult.forResultSet(rs);
            assertThat(result).containsOnly(TpchTableResults.NATION_ROWS);
        }
    }

    @Test
    void shouldRunQueryWithFileAuthenticator(LdapAndFileEnvironment env)
            throws Exception
    {
        // Use file-based password to authenticate (same user, different password)
        try (Connection conn = env.createTrinoConnection(env.getDefaultUser(), env.getFileUserPassword());
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(NATION_SELECT_ALL_QUERY)) {
            QueryResult result = QueryResult.forResultSet(rs);
            assertThat(result).containsOnly(TpchTableResults.NATION_ROWS);
        }
    }

    @Test
    void shouldRunQueryForFileOnlyUser(LdapAndFileEnvironment env)
            throws Exception
    {
        // Use file-only user (not in LDAP)
        try (Connection conn = env.createTrinoConnection(env.getOnlyFileUser(), env.getOnlyFileUserPassword());
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(NATION_SELECT_ALL_QUERY)) {
            QueryResult result = QueryResult.forResultSet(rs);
            assertThat(result).containsOnly(TpchTableResults.NATION_ROWS);
        }
    }

    @Test
    void shouldRunQueryForAnotherUserWithOnlyFileAuthenticator(LdapAndFileEnvironment env)
            throws Exception
    {
        // Legacy parity alias for file-only user authentication coverage.
        shouldRunQueryForFileOnlyUser(env);
    }
}
