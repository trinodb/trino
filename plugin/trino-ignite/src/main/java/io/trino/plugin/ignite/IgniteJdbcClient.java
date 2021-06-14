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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.plugin.jdbc.BaseJdbcClient;
import io.trino.plugin.jdbc.BaseJdbcConfig;
import io.trino.plugin.jdbc.ColumnMapping;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.JdbcOutputTableHandle;
import io.trino.plugin.jdbc.JdbcTableHandle;
import io.trino.plugin.jdbc.JdbcTypeHandle;
import io.trino.plugin.jdbc.RemoteTableName;
import io.trino.plugin.jdbc.WriteMapping;
import io.trino.plugin.jdbc.mapping.IdentifierMapping;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.type.Type;

import javax.annotation.Nullable;
import javax.inject.Inject;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;
import static io.trino.plugin.jdbc.StandardColumnMappings.varcharColumnMapping;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static java.lang.String.format;
import static java.lang.String.join;
import static java.util.Objects.requireNonNull;

public class IgniteJdbcClient
        extends BaseJdbcClient
{
    /**
     * Ignite only support two schemas: sys and public.
     * The sys schema is a view of all metadata about the database. user tables are all under the schema public.
     */
    private static final String IGNITE_SCHEMA = "public";

    @Inject
    public IgniteJdbcClient(BaseJdbcConfig config, ConnectionFactory connectionFactory, IdentifierMapping identifierMapping)
    {
        super(config, "`", connectionFactory, identifierMapping);
    }

    @Override
    public Collection<String> listSchemas(Connection connection)
    {
        return ImmutableList.of(IGNITE_SCHEMA);
    }

    @Override
    public ResultSet getTables(Connection connection, Optional<String> schemaName, Optional<String> tableName)
            throws SQLException
    {
        DatabaseMetaData metadata = connection.getMetaData();
        return metadata.getTables(connection.getCatalog(),
                IGNITE_SCHEMA,
                tableName.orElse(null),
                null);
    }

    @Override
    public Optional<ColumnMapping> toColumnMapping(ConnectorSession session, Connection connection, JdbcTypeHandle typeHandle)
    {
        switch (typeHandle.getJdbcType()) {
            case Types.VARCHAR:
                return Optional.of(varcharColumnMapping(createUnboundedVarcharType(), false));
        }
        return legacyColumnMapping(session, connection, typeHandle);
    }

    @Override
    protected String createTableSql(RemoteTableName remoteTableName, List<String> columns, ConnectorTableMetadata tableMetadata)
    {
        ImmutableList.Builder<String> tableOptions = ImmutableList.builder();
        ImmutableList.Builder<String> columnDefinitions = ImmutableList.builder();
        Map<String, Object> tableProperties = tableMetadata.getProperties();
        columnDefinitions.addAll(columns);

        List<String> primaryKeys = IgniteTableProperties.getPrimaryKey(tableProperties);
        checkArgument(primaryKeys != null && !primaryKeys.isEmpty(), "No primary key defined for create table");
        checkArgument(primaryKeys.size() < columns.size(), "Ignite table must have at least one non PRIMARY KEY column.");
        columnDefinitions.add("PRIMARY KEY (" + join(", ", primaryKeys) + ")");

        for (Map.Entry<String, Object> propertyEntry : tableProperties.entrySet()) {
            String propertyKey = propertyEntry.getKey();
            switch (propertyKey) {
                case IgniteTableProperties.PRIMARY_KEY_PROPERTY:
                    break;
                case IgniteTableProperties.AFFINITY_KEY_PROPERTY:
                    String affinityKey = IgniteTableProperties.getAffinityKey(tableProperties);
                    if (!isNullOrEmpty(affinityKey)) {
                        checkArgument(ImmutableSet.copyOf(primaryKeys).contains(affinityKey), "Affinity key should be one of the primary key");
                        tableOptions.add(propertyKey + " = " + affinityKey);
                    }
                    break;
                case IgniteTableProperties.BACK_UPS_PROPERTY:
                case IgniteTableProperties.TEMPLATE_PROPERTY:
                case IgniteTableProperties.CACHE_GROUP_PROPERTY:
                case IgniteTableProperties.CACHE_NAME_PROPERTY:
                case IgniteTableProperties.DATA_REGION_PROPERTY:
                case IgniteTableProperties.WRITE_SYNCHRONIZATION_MODE_PROPERTY:
                    tableOptions.add(propertyKey + " = " + propertyEntry.getValue());
                    break;
                default:
                    throw new IllegalStateException("Unexpected value: " + propertyKey);
            }
        }

        return format("CREATE TABLE %s (%s) WITH \" %s \"", quoted(remoteTableName), join(", ", columnDefinitions.build()), join(", ", tableOptions.build()));
    }

    @Override
    protected String quoted(@Nullable String catalog, @Nullable String schema, String table)
    {
        StringBuilder sb = new StringBuilder();
        if (!isNullOrEmpty(schema)) {
            sb.append(quoted(schema)).append(".");
        }
        sb.append(quoted(table));
        return sb.toString();
    }

    @Override
    protected Optional<BiFunction<String, Long, String>> limitFunction()
    {
        return Optional.of((sql, limit) -> sql + " LIMIT " + limit);
    }

    @Override
    public boolean isLimitGuaranteed(ConnectorSession session)
    {
        return true;
    }

    @Override
    public Map<String, Object> getTableProperties(ConnectorSession session, JdbcTableHandle tableHandle)
    {
        ImmutableMap.Builder<String, Object> properties = ImmutableMap.builder();
        SchemaTableName schemaTableName = tableHandle.asPlainTable().getSchemaTableName();
        checkArgument(schemaTableName != null && schemaTableName.getSchemaName().equalsIgnoreCase("public"), "Ignite only support public schema");
        String tableName = requireNonNull(schemaTableName.getTableName(), "Ignite table name can not be null").toUpperCase(Locale.ENGLISH);
        String sql = format("SELECT idx.CACHE_ID, " +
                "che.CACHE_MODE as TEMPLATE, " +
                "che.WRITE_SYNCHRONIZATION_MODE, " +
                "che.ATOMICITY_MODE as ATOMICITY, " +
                "che.DATA_REGION_NAME as DATA_REGION, " +
                "che.CACHE_GROUP_NAME as CACHE_GROUP, " +
                "che.CACHE_NAME, " +
                "che.BACKUPS, " +
                "(select COLUMNS FROM sys.indexes WHERE SCHEMA_NAME = 'PUBLIC' AND TABLE_NAME = '%s' AND INDEX_NAME = '_key_PK') as PKS," +
                "(select COLUMNS FROM sys.indexes WHERE SCHEMA_NAME = 'PUBLIC' and TABLE_NAME = '%s' and INDEX_NAME = 'AFFINITY_KEY') as AFK FROM sys.indexes as idx " +
                "JOIN sys.caches che ON idx.CACHE_ID = che.CACHE_ID WHERE idx.SCHEMA_NAME = 'PUBLIC' AND idx.TABLE_NAME = '%s' LIMIT 1", tableName, tableName, tableName);

        try (Connection connection = connectionFactory.openConnection(session)) {
            try (PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
                ResultSet resultSet = preparedStatement.executeQuery();
                checkArgument(resultSet.next(), "Ignite table: '" + tableName + "' properties is NULL");
                List<String> primaryKeys = extract(resultSet.getString("PKS"));
                checkArgument(!primaryKeys.isEmpty(), "Ignite table should has at least one primary key");
                properties.put(IgniteTableProperties.PRIMARY_KEY_PROPERTY, primaryKeys);

                for (String property : IgniteTableProperties.WITH_PROPERTIES) {
                    switch (property) {
                        case IgniteTableProperties.AFFINITY_KEY_PROPERTY:
                            List<String> affinityKeys = extract(resultSet.getString("AFK"));
                            if (!affinityKeys.isEmpty()) {
                                String affinityKey = affinityKeys.get(0);
                                checkArgument(ImmutableSet.copyOf(primaryKeys).contains(affinityKey), "Table affinity key should be one of the primary key");
                                properties.put(property, affinityKey);
                            }
                            break;
                        case IgniteTableProperties.BACK_UPS_PROPERTY:
                            int backups = resultSet.getInt(property.toUpperCase());
                            if (backups > 0) {
                                properties.put(property, backups);
                            }
                            break;
                        case IgniteTableProperties.TEMPLATE_PROPERTY:
                            Optional.ofNullable(resultSet.getString(property.toUpperCase()))
                                    .map(IgniteTemplateType::valueOf)
                                    .ifPresent(value -> properties.put(property, value));
                            break;
                        case IgniteTableProperties.WRITE_SYNCHRONIZATION_MODE_PROPERTY:
                            Optional.ofNullable(resultSet.getString(property.toUpperCase()))
                                    .map(IgniteWriteSyncMode::valueOf)
                                    .ifPresent(value -> properties.put(property, value));
                            break;
                        case IgniteTableProperties.CACHE_NAME_PROPERTY:
                        case IgniteTableProperties.CACHE_GROUP_PROPERTY:
                        case IgniteTableProperties.DATA_REGION_PROPERTY:
                            Optional.ofNullable(resultSet.getString(property.toUpperCase())).ifPresent(value -> properties.put(property, value));
                            break;
                        default:
                            throw new IllegalStateException("Unexpected value: " + property);
                    }
                }
                return properties.build();
            }
        }
        catch (SQLException throwables) {
            throwables.printStackTrace();
        }
        return properties.build();
    }

    // extract result from : "ID" ASC, "CITY_ID" ASC
    private List<String> extract(String row)
    {
        ImmutableList.Builder<String> builder = ImmutableList.builder();
        if (isNullOrEmpty(row)) {
            return builder.build();
        }
        for (String key : row.split(",")) {
            int left = key.indexOf("\"") + 1;
            int right = key.lastIndexOf("\"");
            builder.add(key.substring(left, right).toLowerCase(Locale.ENGLISH));
        }
        return builder.build();
    }

    @Override
    public WriteMapping toWriteMapping(ConnectorSession session, Type type)
    {
        return legacyToWriteMapping(session, type);
    }

    @Override
    public JdbcOutputTableHandle beginCreateTable(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support creating tables with data");
    }

    @Override
    public void createSchema(ConnectorSession session, String schemaName)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support creating schemas");
    }

    @Override
    public void dropSchema(ConnectorSession session, String schemaName)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support dropping schemas");
    }
}
