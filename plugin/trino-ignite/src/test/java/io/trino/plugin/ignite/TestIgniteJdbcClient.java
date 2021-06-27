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
package io.trino.plugin.ignite;

import io.trino.plugin.jdbc.BaseJdbcConfig;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.credential.EmptyCredentialProvider;
import io.trino.plugin.jdbc.mapping.DefaultIdentifierMapping;

import java.sql.SQLException;

public class TestIgniteJdbcClient
        implements AutoCloseable
{
    private final IgniteJdbcClient igniteJdbcClient;

    public TestIgniteJdbcClient(String connectionUrl)
            throws SQLException
    {
        BaseJdbcConfig baseJdbcConfig = new BaseJdbcConfig().setConnectionUrl(connectionUrl);
        ConnectionFactory connectionFactory = IgniteJdbcClientModule.createConnectionFactory(baseJdbcConfig, new EmptyCredentialProvider());
        igniteJdbcClient = new IgniteJdbcClient(
                baseJdbcConfig,
                connectionFactory,
                new DefaultIdentifierMapping());
    }

    @Override
    public void close()
            throws Exception
    {
    }
}
