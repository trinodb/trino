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
import io.trino.tempto.ProductTest;
import io.trino.tempto.Requirement;
import io.trino.tempto.RequirementsProvider;
import io.trino.tempto.configuration.Configuration;
import io.trino.tempto.query.QueryResult;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static com.google.common.base.Preconditions.checkState;
import static io.trino.tests.product.ImmutableLdapObjectDefinitions.getLdapRequirement;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public abstract class BaseLdapJdbcTest
        extends ProductTest
        implements RequirementsProvider
{
    protected static final long TIMEOUT = 30 * 1000; // seconds per test

    protected static final String NATION_SELECT_ALL_QUERY = "SELECT * FROM tpch.tiny.nation";

    @Inject
    @Named("databases.trino.cli_ldap_truststore_path")
    protected String ldapTruststorePath;

    @Inject
    @Named("databases.trino.cli_ldap_truststore_password")
    protected String ldapTruststorePassword;

    @Inject
    @Named("databases.trino.cli_ldap_user_name")
    protected String ldapUserName;

    @Inject
    @Named("databases.trino.cli_ldap_user_password")
    protected String ldapUserPassword;

    @Inject
    @Named("databases.trino.cli_ldap_server_address")
    private String trinoServer;

    @Override
    public Requirement getRequirements(Configuration configuration)
    {
        return getLdapRequirement();
    }

    protected void expectQueryToFail(String user, String password, String message)
    {
        assertThatThrownBy(() -> executeLdapQuery(NATION_SELECT_ALL_QUERY, user, password))
                .isInstanceOf(SQLException.class)
                .hasMessageContaining(message);
    }

    protected QueryResult executeLdapQuery(String query, String name, String password)
            throws SQLException
    {
        try (Connection connection = getLdapConnection(name, password)) {
            Statement statement = connection.createStatement();
            ResultSet rs = statement.executeQuery(query);
            return QueryResult.forResultSet(rs);
        }
    }

    private Connection getLdapConnection(String name, String password)
            throws SQLException
    {
        return DriverManager.getConnection(getLdapUrl(), name, password);
    }

    protected String trinoServer()
    {
        String prefix = "https://";
        checkState(trinoServer.startsWith(prefix), "invalid server address: %s", trinoServer);
        return trinoServer.substring(prefix.length());
    }

    protected String getLdapUrl()
    {
        return format(getLdapUrlFormat(), trinoServer(), ldapTruststorePath, ldapTruststorePassword);
    }

    protected abstract String getLdapUrlFormat();
}
