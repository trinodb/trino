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
package com.starburstdata.presto.plugin.snowflake;

import com.google.common.collect.ImmutableList;
import com.starburstdata.presto.plugin.snowflake.distributed.SnowflakeDistributedConnectorFactory;
import com.starburstdata.presto.plugin.snowflake.jdbc.SnowflakeJdbcClientModule;
import io.prestosql.plugin.base.LicenceCheckingConnectorFactory;
import io.prestosql.plugin.jdbc.JdbcConnectorFactory;
import io.prestosql.plugin.jdbc.JdbcDiagnosticModule;
import io.prestosql.plugin.jdbc.JdbcModule;
import io.prestosql.plugin.jdbc.credential.CredentialProviderModule;
import io.prestosql.spi.NonObfuscable;
import io.prestosql.spi.Plugin;
import io.prestosql.spi.connector.ConnectorFactory;

@NonObfuscable
public class SnowflakePlugin
        implements Plugin
{
    static final String SNOWFLAKE_JDBC = "snowflake-jdbc";
    static final String SNOWFLAKE_DISTRIBUTED = "snowflake-distributed";

    @Override
    public Iterable<ConnectorFactory> getConnectorFactories()
    {
        return ImmutableList.of(
                requireLicense(new JdbcConnectorFactory(
                        SNOWFLAKE_JDBC,
                        catalogName -> {
                            return ImmutableList.of(
                                    new JdbcModule(),
                                    new JdbcDiagnosticModule(catalogName),
                                    new CredentialProviderModule(),
                                    new SnowflakeJdbcClientModule(catalogName, false));
                        },
                        getClass().getClassLoader())),
                requireLicense(new SnowflakeDistributedConnectorFactory(SNOWFLAKE_DISTRIBUTED)));
    }

    private LicenceCheckingConnectorFactory requireLicense(ConnectorFactory connectorFactory)
    {
        return new LicenceCheckingConnectorFactory(connectorFactory, "snowflake");
    }
}
