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
package io.trino.plugin.mysql;

import io.trino.plugin.jdbc.DefaultJdbcMetadata;
import io.trino.plugin.jdbc.JdbcClient;
import io.trino.plugin.jdbc.JdbcColumnHandle;
import io.trino.plugin.jdbc.JdbcQueryEventListener;
import io.trino.plugin.jdbc.JdbcTableHandle;
import io.trino.plugin.jdbc.RemoteTableName;
import io.trino.plugin.jdbc.TimestampTimeZoneDomain;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.SchemaTableName;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.jdbc.JdbcMetadata.getColumns;

public class MySqlMetadata
        extends DefaultJdbcMetadata
{
    private final JdbcClient mySqlClient;

    public MySqlMetadata(JdbcClient mySqlClient, TimestampTimeZoneDomain timestampTimeZoneDomain, Set<JdbcQueryEventListener> jdbcQueryEventListeners)
    {
        super(mySqlClient, timestampTimeZoneDomain, true, jdbcQueryEventListeners);
        this.mySqlClient = mySqlClient;
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table)
    {
        JdbcTableHandle handle = (JdbcTableHandle) table;
        SchemaTableName schemaTableName = handle.getRequiredNamedRelation().getSchemaTableName();
        RemoteTableName remoteTableName = handle.getRequiredNamedRelation().getRemoteTableName();
        return new ConnectorTableMetadata(
                schemaTableName,
                mySqlClient.getColumns(session, schemaTableName, remoteTableName).stream()
                        .map(MySqlMetadata::toColumnMetadata)
                        .collect(toImmutableList()),
                mySqlClient.getTableProperties(session, handle),
                getTableComment(handle));
    }

    @Override
    public List<ColumnMetadata> getColumnMetadata(ConnectorSession session, JdbcTableHandle handle)
    {
        return getColumns(session, mySqlClient, handle).stream()
                .map(MySqlMetadata::toColumnMetadata)
                .collect(toImmutableList());
    }

    private static ColumnMetadata toColumnMetadata(JdbcColumnHandle handle)
    {
        ColumnMetadata columnMetadata = handle.getColumnMetadata();
        ColumnMetadata.Builder columnMetadataBuilder = ColumnMetadata.builderFrom(columnMetadata);

        Map<String, Object> properties = new LinkedHashMap<>(columnMetadata.getProperties());

        StringBuilder extraBuilder = new StringBuilder();
        String extraInfo = columnMetadata.getExtraInfo();
        if (extraInfo != null && !extraInfo.isEmpty()) {
            extraBuilder.append(extraInfo);
        }

        if (handle.isAutoIncrement()) {
            properties.put(MySqlColumnProperties.AUTO_INCREMENT, true);
            if (!extraBuilder.isEmpty()) {
                extraBuilder.append(", ");
            }
            extraBuilder.append(MySqlColumnProperties.AUTO_INCREMENT).append("=true");
        }

        columnMetadataBuilder.setProperties(properties);

        if (!extraBuilder.isEmpty()) {
            columnMetadataBuilder.setExtraInfo(Optional.of(extraBuilder.toString()));
        }
        return columnMetadataBuilder.build();
    }
}
