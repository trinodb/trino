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

import io.trino.testing.containers.environment.ProductTest;
import io.trino.testing.containers.environment.QueryResult;
import io.trino.testing.containers.environment.RequiresEnvironment;
import io.trino.testing.containers.environment.TpchTableResults;
import io.trino.tests.product.TestGroup;
import org.ietf.jgss.GSSCredential;
import org.ietf.jgss.GSSException;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static io.trino.testing.containers.environment.QueryResultAssert.assertThat;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for JDBC Kerberos constrained delegation authentication.
 * <p>
 * Migrated from the Tempto-based TestKerberosConstrainedDelegationJdbc to JUnit 5.
 * <p>
 * These tests verify that Kerberos constrained delegation works correctly,
 * including handling of disposed and expired credentials.
 */
@ProductTest
@RequiresEnvironment(JdbcKerberosEnvironment.class)
@TestGroup.JdbcKerberosConstrainedDelegation
class TestKerberosConstrainedDelegationJdbc
{
    @Test
    void testSelectConstrainedDelegationKerberos(JdbcKerberosEnvironment env)
            throws Exception
    {
        GSSCredential credential = env.createGssCredential();
        try (Connection connection = env.createConnectionWithKerberosDelegation(credential);
                PreparedStatement statement = connection.prepareStatement("SELECT * FROM tpch.tiny.nation");
                ResultSet results = statement.executeQuery()) {
            QueryResult result = QueryResult.forResultSet(results);
            assertThat(result).containsOnly(TpchTableResults.NATION_ROWS);
        }
        finally {
            credential.dispose();
        }
    }

    @Test
    void testCtasConstrainedDelegationKerberos(JdbcKerberosEnvironment env)
            throws Exception
    {
        GSSCredential credential = env.createGssCredential();
        try (Connection connection = env.createConnectionWithKerberosDelegation(credential);
                PreparedStatement statement = connection.prepareStatement(
                        "CREATE TABLE memory.default.test_kerberos_ctas AS SELECT * FROM tpch.tiny.nation")) {
            int results = statement.executeUpdate();
            assertThat(results).isEqualTo(TpchTableResults.NATION_ROW_COUNT);
        }
        finally {
            try {
                // Cleanup the created table
                GSSCredential cleanupCredential = env.createGssCredential();
                try (Connection connection = env.createConnectionWithKerberosDelegation(cleanupCredential);
                        Statement statement = connection.createStatement()) {
                    statement.execute("DROP TABLE IF EXISTS memory.default.test_kerberos_ctas");
                }
                finally {
                    cleanupCredential.dispose();
                }
            }
            catch (Exception e) {
                // Ignore cleanup errors
            }
            credential.dispose();
        }
    }

    @Test
    void testQueryOnDisposedCredential(JdbcKerberosEnvironment env)
            throws Exception
    {
        GSSCredential credential = env.createGssCredential();
        credential.dispose();

        try (Connection connection = env.createConnectionWithKerberosDelegation(credential)) {
            assertThatThrownBy(() -> connection.prepareStatement("SELECT * FROM tpch.tiny.nation"))
                    .cause()
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("This credential is no longer valid");
        }
    }

    @Test
    void testQueryOnExpiredCredential(JdbcKerberosEnvironment env)
            throws Exception
    {
        GSSCredential credential = env.createGssCredential();
        try {
            // Ticket default lifetime is 80s by krb.conf, sleep to expire ticket
            // The MIN_LIFETIME on client side is 60s
            Thread.sleep(30000);

            // Check before execution that current lifetime is less than 60s (MIN_LIFETIME on client),
            // to be sure that we already expired
            assertThat(credential.getRemainingLifetime()).isLessThanOrEqualTo(60);

            try (Connection connection = env.createConnectionWithKerberosDelegation(credential)) {
                assertThatThrownBy(() -> connection.prepareStatement("SELECT * FROM tpch.tiny.nation"))
                        .isInstanceOf(SQLException.class)
                        .hasMessageContaining("Kerberos credential is expired");
            }
        }
        finally {
            try {
                credential.dispose();
            }
            catch (GSSException e) {
                // Ignore - credential may already be invalid
            }
        }
    }
}
