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
package io.trino.plugin.jdbc;

import com.google.inject.Inject;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.DynamicFilter;

import static io.trino.plugin.jdbc.JdbcDynamicFilteringSessionProperties.dynamicFilteringEnabled;
import static java.util.Objects.requireNonNull;

public class JdbcSplitManager
        implements ConnectorSplitManager
{
    private final JdbcClient jdbcClient;

    @Inject
    public JdbcSplitManager(JdbcClient jdbcClient)
    {
        this.jdbcClient = requireNonNull(jdbcClient, "jdbcClient is null");
    }

    @Override
    public ConnectorSplitSource getSplits(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorTableHandle table,
            DynamicFilter dynamicFilter,
            Constraint constraint)
    {
        if (table instanceof JdbcProcedureHandle procedureHandle) {
            return jdbcClient.getSplits(session, procedureHandle);
        }

        JdbcTableHandle tableHandle = (JdbcTableHandle) table;
        ConnectorSplitSource jdbcSplitSource = jdbcClient.getSplits(session, tableHandle);
        if (dynamicFilteringEnabled(session)) {
            return new DynamicFilteringJdbcSplitSource(jdbcSplitSource, dynamicFilter, tableHandle);
        }
        return jdbcSplitSource;
    }
}
