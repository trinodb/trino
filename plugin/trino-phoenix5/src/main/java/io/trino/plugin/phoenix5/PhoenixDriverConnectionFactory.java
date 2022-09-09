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
package io.trino.plugin.phoenix5;

import io.airlift.log.Logger;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.credential.CredentialPropertiesProvider;
import io.trino.plugin.jdbc.credential.CredentialProvider;
import io.trino.plugin.jdbc.credential.DefaultCredentialPropertiesProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.security.ConnectorIdentity;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.phoenix.jdbc.PhoenixEmbeddedDriver;
import org.apache.phoenix.query.QueryServices;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;
import java.util.*;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class PhoenixDriverConnectionFactory
    implements ConnectionFactory
{
    private final Driver driver;
    private final String connectionUrl;
    private final Properties connectionProperties;
    private final CredentialPropertiesProvider<String, String> credentialPropertiesProvider;
    private UserGroupInformation userGroupInformation;
    private final PhoenixConfig config;
    private static final Set<String> proxyUserSet = new HashSet<>();

    public PhoenixDriverConnectionFactory(Driver driver, String connectionUrl, PhoenixConfig config, CredentialProvider credentialProvider)
            throws SQLException
    {
        this(driver, connectionUrl, config, new DefaultCredentialPropertiesProvider(credentialProvider));
    }

    public PhoenixDriverConnectionFactory(Driver driver, String connectionUrl, PhoenixConfig config, CredentialPropertiesProvider<String, String> credentialPropertiesProvider)
            throws SQLException
    {
        this.driver = requireNonNull(driver, "driver is null");
        this.connectionUrl = requireNonNull(connectionUrl, "connectionUrl is null");
        this.connectionProperties = new Properties();
        this.config = requireNonNull(config, "config is null");
        this.connectionProperties.putAll(requireNonNull(getConnectionProperties(config), "connectionProperties is null"));
        this.credentialPropertiesProvider = requireNonNull(credentialPropertiesProvider, "credentialPropertiesProvider is null");
    }

    @Override
    public Connection openConnection(ConnectorSession session)
            throws SQLException
    {
        Properties properties = getCredentialProperties(session.getIdentity());
        Connection[] connection = new Connection[1];
        String user = session.getIdentity().getUser();

        if (config.isHbaseImpersonationEnabled()) {
            try {
                if(!proxyUserSet.contains(user)) {
                    userGroupInformation = UserGroupInformation.createProxyUser(user, UserGroupInformation.getLoginUser());
                    proxyUserSet.add(user);
                }
                userGroupInformation.doAs(new PrivilegedExceptionAction<Connection>() {
                    @Override
                    public Connection run() throws Exception {
                        connection[0] = driver.connect(connectionUrl, properties);
                        return connection[0];
                    }
                });
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        } else {
            connection[0] = driver.connect(connectionUrl, properties);
        }
        checkState(connection[0] != null, "Driver returned null connection, make sure the connection URL '%s' is valid for the driver %s", connectionUrl, driver);
        return connection[0];
    }

    private Properties getCredentialProperties(ConnectorIdentity identity)
    {
        Properties properties = new Properties();
        properties.putAll(connectionProperties);
        properties.putAll(credentialPropertiesProvider.getCredentialProperties(identity));
        return properties;
    }

    public static Properties getConnectionProperties(PhoenixConfig config)
            throws SQLException
    {
        Configuration resourcesConfig = readConfig(config);
        Properties connectionProperties = new Properties();
        for (Map.Entry<String, String> entry : resourcesConfig) {
            connectionProperties.setProperty(entry.getKey(), entry.getValue());
        }

        PhoenixEmbeddedDriver.ConnectionInfo connectionInfo = PhoenixEmbeddedDriver.ConnectionInfo.create(config.getConnectionUrl());
        connectionInfo.asProps().asMap().forEach(connectionProperties::setProperty);

        if(config.isHbaseKerberosEnabled()) {
            Configuration conf = HBaseConfiguration.create();
            connectionProperties.forEach((k, v) -> conf.set((String) k, (String) v));
            conf.set("hadoop.security.authentication", "Kerberos");
            UserGroupInformation.setConfiguration(conf);
            try {
                kerberosAuthentication(conf);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        return connectionProperties;
    }

    private static Configuration readConfig(PhoenixConfig config)
    {
        Configuration result = new Configuration(false);
        for (String resourcePath : config.getResourceConfigFiles()) {
            result.addResource(new Path(resourcePath));
        }
        return result;
    }

    private static void kerberosAuthentication(Configuration conf)
            throws IOException
    {
        String keytabFilename = conf.get(QueryServices.HBASE_CLIENT_KEYTAB);
        if (keytabFilename == null || keytabFilename.length() == 0) {
            throw new IOException("Running in secure mode, but config doesn't have a keytab");
        }
        String principalConfig = conf.get(QueryServices.HBASE_CLIENT_PRINCIPAL, System.getProperty("user.name"));
        String principalName = SecurityUtil.getServerPrincipal(principalConfig, "0.0.0.0");
        UserGroupInformation.loginUserFromKeytab(principalName, keytabFilename);
    }
}
