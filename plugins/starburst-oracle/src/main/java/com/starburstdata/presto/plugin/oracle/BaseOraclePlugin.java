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
package com.starburstdata.presto.plugin.oracle;

import com.google.common.collect.ImmutableList;
import io.prestosql.plugin.base.LicenceCheckingConnectorFactory;
import io.prestosql.plugin.jdbc.JdbcConnectorFactory;
import io.prestosql.plugin.jdbc.JdbcDiagnosticModule;
import io.prestosql.plugin.jdbc.JdbcModule;
import io.prestosql.spi.Plugin;
import io.prestosql.spi.connector.ConnectorFactory;

public class BaseOraclePlugin
        implements Plugin
{
    @Override
    public Iterable<ConnectorFactory> getConnectorFactories()
    {
        JdbcConnectorFactory connectorFactory = new JdbcConnectorFactory(
                "oracle",
                catalogName -> {
                    return ImmutableList.of(
                            new JdbcModule(),
                            new JdbcDiagnosticModule(catalogName),
                            new OracleAuthenticationModule(catalogName),
                            new OracleClientModule());
                },
                BaseOraclePlugin.class.getClassLoader());
        return ImmutableList.of(new LicenceCheckingConnectorFactory(connectorFactory, "oracle"));
    }
}
