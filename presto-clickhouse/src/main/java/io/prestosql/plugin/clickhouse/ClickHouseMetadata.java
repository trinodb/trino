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
package io.prestosql.plugin.clickhouse;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.prestosql.plugin.jdbc.JdbcColumnHandle;
import io.prestosql.plugin.jdbc.JdbcIdentity;
import io.prestosql.plugin.jdbc.JdbcMetadata;
import io.prestosql.plugin.jdbc.JdbcMetadataConfig;
import io.prestosql.plugin.jdbc.JdbcTableHandle;
import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorTableHandle;
import io.prestosql.spi.connector.ConnectorTableMetadata;
import io.prestosql.spi.connector.SchemaTableName;
import ru.yandex.clickhouse.util.ClickHouseFormat;

import javax.inject.Inject;

import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class ClickHouseMetadata
        extends JdbcMetadata
{
    public static final String DEFAULT_SCHEMA = "default";
    private final ClickHouseClient clickhouseClient;
    private static final Logger log = Logger.get(ClickHouseClient.class);

    @Inject
    public ClickHouseMetadata(ClickHouseClient clickhouseclient, JdbcMetadataConfig metadataConfig)
    {
        super(clickhouseclient, metadataConfig.isAllowDropTable());
        this.clickhouseClient = requireNonNull(clickhouseclient, "client is null");
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return ImmutableList.copyOf(clickhouseClient.getSchemaNames(JdbcIdentity.from(session)));
    }

    @Override
    public JdbcTableHandle getTableHandle(ConnectorSession session, SchemaTableName schemaTableName)
    {
        return clickhouseClient.getTableHandle(JdbcIdentity.from(session), schemaTableName)
                .map(tableHandle -> new JdbcTableHandle(
                        schemaTableName,
                        tableHandle.getCatalogName(),
                        tableHandle.getSchemaName(),
                        tableHandle.getTableName()))
                .orElse(null);
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table)
    {
        log.info("entering into getTableMetadata.......");
        JdbcTableHandle handle = (JdbcTableHandle) table;

        ImmutableMap.Builder<String, Object> properties = ImmutableMap.builder();
        properties.put(ClickHouseFormat.Native.name(), ClickHouseTableProperties.ENGINE_PROPERTY);
        List<ColumnMetadata> columnMetadata = clickhouseClient.getColumns(session, handle).stream()
                .map(JdbcColumnHandle::getColumnMetadata)
                .collect(toImmutableList());
        return new ConnectorTableMetadata(handle.getSchemaTableName(), columnMetadata, properties.build());
    }
}
