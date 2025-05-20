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

import com.google.common.collect.ImmutableList;
import dev.failsafe.RetryPolicy;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.RecordCursor;
import io.trino.spi.connector.RecordSet;
import io.trino.spi.type.Type;

import java.util.List;
import java.util.concurrent.ExecutorService;

import static io.trino.plugin.jdbc.RetryingModule.retry;
import static java.util.Objects.requireNonNull;

public class JdbcRecordSet
        implements RecordSet
{
    private final JdbcClient jdbcClient;
    private final ExecutorService executor;
    private final BaseJdbcConnectorTableHandle table;
    private final List<JdbcColumnHandle> columnHandles;
    private final List<Type> columnTypes;
    private final JdbcSplit split;
    private final ConnectorSession session;
    private final RetryPolicy<Object> policy;

    public JdbcRecordSet(
            JdbcClient jdbcClient,
            ExecutorService executor,
            ConnectorSession session,
            RetryPolicy<Object> policy,
            JdbcSplit split,
            BaseJdbcConnectorTableHandle table,
            List<JdbcColumnHandle> columnHandles)
    {
        this.jdbcClient = requireNonNull(jdbcClient, "jdbcClient is null");
        this.executor = requireNonNull(executor, "executor is null");
        this.split = requireNonNull(split, "split is null");

        this.table = requireNonNull(table, "table is null");
        this.columnHandles = requireNonNull(columnHandles, "columnHandles is null");
        ImmutableList.Builder<Type> types = ImmutableList.builderWithExpectedSize(columnHandles.size());
        for (JdbcColumnHandle column : columnHandles) {
            types.add(column.getColumnType());
        }
        this.columnTypes = types.build();
        this.session = requireNonNull(session, "session is null");
        this.policy = requireNonNull(policy, "policy is null");
    }

    @Override
    public List<Type> getColumnTypes()
    {
        return columnTypes;
    }

    @Override
    public RecordCursor cursor()
    {
        return retry(policy, () -> new JdbcRecordCursor(jdbcClient, executor, session, split, table, columnHandles));
    }
}
