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

import com.google.inject.Inject;
import com.google.inject.name.Named;
import io.trino.tempto.BeforeMethodWithContext;
import io.trino.tempto.ProductTest;
import io.trino.tempto.kerberos.KerberosAuthentication;
import io.trino.tests.product.TpchTableResults;
import org.assertj.core.api.Assertions;
import org.ietf.jgss.GSSCredential;
import org.ietf.jgss.GSSManager;
import org.ietf.jgss.Oid;
import org.testng.annotations.Test;

import javax.security.auth.Subject;

import java.security.Principal;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.tempto.assertions.QueryAssert.assertThat;
import static io.trino.tempto.query.QueryResult.forResultSet;
import static io.trino.tests.product.TestGroups.JDBC_KERBEROS_CONSTRAINED_DELEGATION;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.ietf.jgss.GSSCredential.DEFAULT_LIFETIME;
import static org.ietf.jgss.GSSCredential.INITIATE_ONLY;
import static org.ietf.jgss.GSSName.NT_USER_NAME;

public class TestKerberosConstrainedDelegationJdbc
        extends ProductTest
{
    private static final String KERBEROS_OID = "1.2.840.113554.1.2.2";
    @Inject
    @Named("databases.presto.jdbc_url")
    String jdbcUrl;

    @Inject
    @Named("databases.presto.kerberos_principal")
    private String kerberosPrincipal;

    @Inject
    @Named("databases.presto.kerberos_keytab")
    private String kerberosKeytab;

    private GSSManager gssManager;
    private KerberosAuthentication kerberosAuthentication;

    @BeforeMethodWithContext
    public void setUp()
    {
        this.gssManager = GSSManager.getInstance();
        this.kerberosAuthentication = new KerberosAuthentication(kerberosPrincipal, kerberosKeytab);
    }

    @Test(groups = JDBC_KERBEROS_CONSTRAINED_DELEGATION)
    public void testSelectConstrainedDelegationKerberos()
            throws Exception
    {
        Properties driverProperties = new Properties();
        GSSCredential credential = createGssCredential();
        driverProperties.put("KerberosConstrainedDelegation", credential);
        try (Connection connection = DriverManager.getConnection(jdbcUrl, driverProperties);
                PreparedStatement statement = connection.prepareStatement("SELECT * FROM tpch.tiny.nation");
                ResultSet results = statement.executeQuery()) {
            assertThat(forResultSet(results)).matches(TpchTableResults.PRESTO_NATION_RESULT);
        }
        finally {
            credential.dispose();
        }
    }

    @Test(groups = JDBC_KERBEROS_CONSTRAINED_DELEGATION)
    public void testCtasConstrainedDelegationKerberos()
            throws Exception
    {
        Properties driverProperties = new Properties();
        GSSCredential credential = createGssCredential();
        driverProperties.put("KerberosConstrainedDelegation", credential);
        try (Connection connection = DriverManager.getConnection(jdbcUrl, driverProperties);
                PreparedStatement statement = connection.prepareStatement(format("CREATE TABLE %s AS SELECT * FROM tpch.tiny.nation", "test_kerberos_ctas"))) {
            int results = statement.executeUpdate();
            Assertions.assertThat(results).isEqualTo(25);
        }
        finally {
            credential.dispose();
        }
    }

    @Test(groups = JDBC_KERBEROS_CONSTRAINED_DELEGATION)
    public void testQueryOnDisposedCredential()
            throws Exception
    {
        Properties driverProperties = new Properties();
        GSSCredential credential = createGssCredential();
        credential.dispose();
        driverProperties.put("KerberosConstrainedDelegation", credential);
        try (Connection connection = DriverManager.getConnection(jdbcUrl, driverProperties)) {
            assertThatThrownBy(() -> connection.prepareStatement("SELECT * FROM tpch.tiny.nation"))
                    .cause()
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("This credential is no longer valid");
        }
    }

    @Test(groups = JDBC_KERBEROS_CONSTRAINED_DELEGATION)
    public void testQueryOnExpiredCredential()
            throws Exception
    {
        Properties driverProperties = new Properties();
        GSSCredential credential = createGssCredential();
        // ticket default lifetime is 80s by kerb.conf, sleep to expire ticket
        Thread.sleep(30000);
        // check before execution that current lifetime is less than 60s (MIN_LIFETIME on client), to be sure that we already expired
        Assertions.assertThat(credential.getRemainingLifetime()).isLessThanOrEqualTo(60);
        driverProperties.put("KerberosConstrainedDelegation", credential);
        try (Connection connection = DriverManager.getConnection(jdbcUrl, driverProperties)) {
            assertThatThrownBy(() -> connection.prepareStatement("SELECT * FROM tpch.tiny.nation"))
                    .isInstanceOf(SQLException.class)
                    .hasMessageContaining("Kerberos credential is expired");
        }
        finally {
            credential.dispose();
        }
    }

    private GSSCredential createGssCredential()
    {
        Subject authenticatedSubject = this.kerberosAuthentication.authenticate();
        Principal clientPrincipal = getOnlyElement(authenticatedSubject.getPrincipals());

        try {
            return Subject.doAs(authenticatedSubject,
                    (PrivilegedExceptionAction<GSSCredential>) () ->
                            gssManager.createCredential(
                                    gssManager.createName(clientPrincipal.getName(), NT_USER_NAME),
                                    DEFAULT_LIFETIME,
                                    new Oid(KERBEROS_OID),
                                    INITIATE_ONLY));
        }
        catch (PrivilegedActionException e) {
            throw new RuntimeException(e.getCause());
        }
    }
}
