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

import com.google.common.collect.ImmutableSet;
import io.trino.plugin.jdbc.DefaultJdbcMetadata;
import io.trino.plugin.jdbc.JdbcClient;
import io.trino.plugin.jdbc.JdbcColumnHandle;
import io.trino.plugin.jdbc.JdbcTableHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableVersion;
import io.trino.spi.connector.ConnectorTableVersioningLayout;
import io.trino.spi.connector.SchemaTableName;

import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.spi.connector.PointerType.TARGET_ID;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.IntegerType.INTEGER;
import static java.util.Objects.requireNonNull;

public class MySqlMetadata
        extends DefaultJdbcMetadata
{
    private static final String DELETE_TABLE_SUFFIX = "$delete";

    private final VersioningService versioningService;

    public MySqlMetadata(JdbcClient jdbcClient, VersioningService versioningService)
    {
        super(jdbcClient, false);
        this.versioningService = requireNonNull(versioningService, "VersioningService is null");
    }

    @Override
    public JdbcTableHandle getTableHandle(
            ConnectorSession session,
            SchemaTableName schemaTableName,
            Optional<ConnectorTableVersion> startVersion,
            Optional<ConnectorTableVersion> endVersion)
    {
        String tableName = schemaTableName.getTableName();
        boolean deletedRows = tableName.endsWith(DELETE_TABLE_SUFFIX);
        if (deletedRows) {
            schemaTableName = new SchemaTableName(
                    schemaTableName.getSchemaName(),
                    tableName.substring(0, tableName.length() - DELETE_TABLE_SUFFIX.length()));
        }

        JdbcTableHandle handle = super.getTableHandle(session, schemaTableName);
        if (handle == null) {
            return null;
        }
        return handle.withVersion(
                        startVersion.map(this::getVersion),
                        endVersion
                                .map(this::getVersion)
                                .or(() -> versioningService.getCurrentTableVersion(handle)))
                .withDeletedRows(deletedRows)
                .withStrictVersioning(endVersion.isPresent() || deletedRows);
    }

    @Override
    public Optional<ConnectorTableVersioningLayout> getTableVersioningLayout(ConnectorSession session, ConnectorTableHandle table)
    {
        JdbcTableHandle handle = (JdbcTableHandle) table;
        if (handle.getEndVersion().isEmpty()) {
            return Optional.empty();
        }
        Set<JdbcColumnHandle> primaryKeys = getColumnHandles(session, table).values().stream()
                .map(JdbcColumnHandle.class::cast)
                .filter(JdbcColumnHandle::isPrimaryKey)
                .collect(toImmutableSet());
        if (primaryKeys.size() != 1) {
            return Optional.empty();
        }
        JdbcColumnHandle primaryKeyColumn = getOnlyElement(primaryKeys);
        if (primaryKeyColumn.getColumnType() != BIGINT && primaryKeyColumn.getColumnType() != INTEGER) {
            return Optional.empty();
        }
        return Optional.of(new ConnectorTableVersioningLayout(handle.withStrictVersioning(true), ImmutableSet.of(primaryKeyColumn), true));
    }

    @Override
    public Optional<ConnectorTableVersion> getCurrentTableVersion(ConnectorSession session, ConnectorTableHandle table)
    {
        JdbcTableHandle handle = (JdbcTableHandle) table;
        return handle.getEndVersion().map(version -> new ConnectorTableVersion(TARGET_ID, BIGINT, version));
    }

    @Override
    public Optional<ConnectorTableHandle> getInsertedOrUpdatedRows(ConnectorSession session, ConnectorTableHandle table, ConnectorTableVersion fromVersionExclusive)
    {
        JdbcTableHandle handle = (JdbcTableHandle) table;
        if (handle.getEndVersion().isEmpty()) {
            return Optional.empty();
        }
        checkArgument(fromVersionExclusive.getPointerType().equals(TARGET_ID));
        checkArgument(fromVersionExclusive.getVersionType().equals(BIGINT));
        return Optional.of(handle
                .withVersion(Optional.of(getVersion(fromVersionExclusive)), handle.getEndVersion())
                .withStrictVersioning(true));
    }

    @Override
    public Optional<ConnectorTableHandle> getDeletedRows(ConnectorSession session, ConnectorTableHandle table, ConnectorTableVersion fromVersionExclusive)
    {
        JdbcTableHandle handle = (JdbcTableHandle) table;
        if (handle.getEndVersion().isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(handle
                .withVersion(Optional.of(getVersion(fromVersionExclusive)), handle.getEndVersion())
                .withDeletedRows(true)
                .withStrictVersioning(true));
    }

    private long getVersion(ConnectorTableVersion version)
    {
        checkArgument(version.getPointerType().equals(TARGET_ID));
        checkArgument(version.getVersionType().equals(BIGINT));
        return (long) version.getVersion();
    }
}
