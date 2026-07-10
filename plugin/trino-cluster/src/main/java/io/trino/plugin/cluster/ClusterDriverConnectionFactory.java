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
package io.trino.plugin.cluster;

import io.opentelemetry.api.OpenTelemetry;
import io.trino.plugin.jdbc.DriverConnectionFactory;
import io.trino.plugin.jdbc.credential.CredentialPropertiesProvider;
import io.trino.plugin.jdbc.credential.CredentialProvider;
import io.trino.plugin.jdbc.credential.DefaultCredentialPropertiesProvider;
import io.trino.spi.connector.ConnectorSession;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;
import java.util.Properties;

public class ClusterDriverConnectionFactory
        extends DriverConnectionFactory
{
    public ClusterDriverConnectionFactory(Driver driver, String connectionUrl, Properties connectionProperties, CredentialPropertiesProvider credentialPropertiesProvider, OpenTelemetry openTelemetry)
    {
        super(driver, connectionUrl, connectionProperties, credentialPropertiesProvider, openTelemetry);
    }

    public static ClusterDriverConnectionFactory create(Driver driver, String connectionUrl, CredentialProvider credentialProvider, Properties connectionProperties, OpenTelemetry openTelemetry)
    {
        return new ClusterDriverConnectionFactory(
                driver,
                connectionUrl,
                connectionProperties,
                new DefaultCredentialPropertiesProvider(credentialProvider),
                openTelemetry);
    }

    @Override
    public Connection openConnection(ConnectorSession session)
            throws SQLException
    {
        Connection connection = super.openConnection(session);
        // For DC (Dynamic Catalog) support: the session catalog name is like "dc.subcatalog".
        // Set the remote catalog so that JDBC metadata queries target the correct sub-catalog.
        String catalogName = session.getCatalog().orElse(null);
        if (catalogName != null) {
            String[] parts = catalogName.split("\\.");
            if (parts.length > 1) {
                connection.setCatalog(parts[1]);
            }
        }
        return connection;
    }
}
