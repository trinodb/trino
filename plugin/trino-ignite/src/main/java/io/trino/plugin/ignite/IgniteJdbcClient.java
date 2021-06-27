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

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.slice.Slice;
import io.trino.plugin.jdbc.BaseJdbcClient;
import io.trino.plugin.jdbc.BaseJdbcConfig;
import io.trino.plugin.jdbc.ColumnMapping;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.JdbcColumnHandle;
import io.trino.plugin.jdbc.JdbcExpression;
import io.trino.plugin.jdbc.JdbcIdentity;
import io.trino.plugin.jdbc.JdbcOutputTableHandle;
import io.trino.plugin.jdbc.JdbcSortItem;
import io.trino.plugin.jdbc.JdbcTableHandle;
import io.trino.plugin.jdbc.JdbcTypeHandle;
import io.trino.plugin.jdbc.LongReadFunction;
import io.trino.plugin.jdbc.LongWriteFunction;
import io.trino.plugin.jdbc.RemoteTableName;
import io.trino.plugin.jdbc.SliceWriteFunction;
import io.trino.plugin.jdbc.WriteMapping;
import io.trino.plugin.jdbc.expression.AggregateFunctionRewriter;
import io.trino.plugin.jdbc.expression.AggregateFunctionRule;
import io.trino.plugin.jdbc.expression.ImplementAvgFloatingPoint;
import io.trino.plugin.jdbc.expression.ImplementCount;
import io.trino.plugin.jdbc.expression.ImplementCountAll;
import io.trino.plugin.jdbc.expression.ImplementMinMax;
import io.trino.plugin.jdbc.expression.ImplementSum;
import io.trino.plugin.jdbc.mapping.IdentifierMapping;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.AggregateFunction;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.type.DateType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;

import javax.annotation.Nullable;
import javax.inject.Inject;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.base.Verify.verify;
import static io.trino.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static io.trino.plugin.jdbc.StandardColumnMappings.shortDecimalWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.varcharColumnMapping;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.VarcharType.UNBOUNDED_LENGTH;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static java.lang.String.format;
import static java.lang.String.join;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class IgniteJdbcClient
        extends BaseJdbcClient
{
    private static final String IGNITE_CATALOG = "IGNITE";
    private static final String IGNITE_SCHEMA = "PUBLIC";
    private final AggregateFunctionRewriter aggregateFunctionRewriter;
    private static final Splitter SPLITTER = Splitter.on(",").omitEmptyStrings().trimResults();

    @Inject
    public IgniteJdbcClient(BaseJdbcConfig config, ConnectionFactory connectionFactory, IdentifierMapping identifierMapping)
    {
        super(config, "`", connectionFactory, identifierMapping);

        JdbcTypeHandle bigintTypeHandle = new JdbcTypeHandle(Types.BIGINT, Optional.of("bigint"), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty());
        this.aggregateFunctionRewriter = new AggregateFunctionRewriter(
                this::quoted,
                ImmutableSet.<AggregateFunctionRule>builder()
                        .add(new ImplementCountAll(bigintTypeHandle))
                        .add(new ImplementCount(bigintTypeHandle))
                        .add(new ImplementMinMax())
                        .add(new ImplementSum(IgniteJdbcClient::toTypeHandle))
                        .add(new ImplementAvgFloatingPoint())
                        .add(new ImplementAvgDecimal())
                        .add(new ImplementAvgBigint())
                        .build());
    }

    @Override
    public Collection<String> listSchemas(Connection connection)
    {
        return ImmutableSet.of(IGNITE_SCHEMA);
    }

    @Override
    public ResultSet getTables(Connection connection, Optional<String> schemaName, Optional<String> tableName)
            throws SQLException
    {
        DatabaseMetaData metadata = connection.getMetaData();
        return metadata.getTables(
                IGNITE_CATALOG,
                IGNITE_SCHEMA,
                tableName.orElse(null),
                new String[] {"TABLE", "VIEW"});
    }

    @Override
    public Optional<ColumnMapping> toColumnMapping(ConnectorSession session, Connection connection, JdbcTypeHandle typeHandle)
    {
        // TODO complete type mapping
        switch (typeHandle.getJdbcType()) {
            case Types.VARCHAR:
                return Optional.of(varcharColumnMapping(
                        typeHandle.getColumnSize()
                                // default added column in Ignite will has Integer.MAX_VALUE length which not allow in Trino, will consider the column as varchar
                                .filter(columnSize -> columnSize != UNBOUNDED_LENGTH)
                                .map(VarcharType::createVarcharType)
                                .orElse(createUnboundedVarcharType()), true));
            case Types.DATE:
                return Optional.of(ColumnMapping.longMapping(
                        DATE,
                        dateReadFunction(),
                        dateWriteFunction()));
        }
        return legacyColumnMapping(session, connection, typeHandle);
    }

    @Override
    public Optional<JdbcExpression> implementAggregation(ConnectorSession session, AggregateFunction aggregate, Map<String, ColumnHandle> assignments)
    {
        return aggregateFunctionRewriter.rewrite(session, aggregate, assignments);
    }

    private static Optional<JdbcTypeHandle> toTypeHandle(DecimalType decimalType)
    {
        return Optional.of(new JdbcTypeHandle(Types.DECIMAL, Optional.of("Decimal"), Optional.of(decimalType.getPrecision()), Optional.of(decimalType.getScale()), Optional.empty(), Optional.empty()));
    }

    // https://issues.apache.org/jira/browse/IGNITE-12824, user do not need to convert between UTC <-> Local time
    public static LongReadFunction dateReadFunction()
    {
        return (resultSet, columnIndex) -> {
            long localMillis = resultSet.getDate(columnIndex).getTime();
            return MILLISECONDS.toDays(localMillis);
        };
    }

    public static LongWriteFunction dateWriteFunction()
    {
        return (statement, index, value) -> {
            long millis = DAYS.toMillis(value);
            statement.setDate(index, new Date(millis));
        };
    }

    @Override
    public JdbcOutputTableHandle beginInsertTable(ConnectorSession session, JdbcTableHandle tableHandle, List<JdbcColumnHandle> columns)
    {
        SchemaTableName schemaTableName = tableHandle.asPlainTable().getSchemaTableName();
        JdbcIdentity identity = JdbcIdentity.from(session);

        try (Connection connection = connectionFactory.openConnection(session)) {
            String remoteSchema = getIdentifierMapping().toRemoteSchemaName(identity, connection, schemaTableName.getSchemaName());
            String remoteTable = getIdentifierMapping().toRemoteTableName(identity, connection, remoteSchema, schemaTableName.getTableName());
            String catalog = connection.getCatalog();

            ImmutableList.Builder<String> columnNames = ImmutableList.builder();
            ImmutableList.Builder<Type> columnTypes = ImmutableList.builder();
            ImmutableList.Builder<JdbcTypeHandle> jdbcColumnTypes = ImmutableList.builder();
            for (JdbcColumnHandle column : columns) {
                columnNames.add(column.getColumnName());
                columnTypes.add(column.getColumnType());
                jdbcColumnTypes.add(column.getJdbcTypeHandle());
            }

            return new JdbcOutputTableHandle(
                    catalog,
                    remoteSchema,
                    remoteTable,
                    columnNames.build(),
                    columnTypes.build(),
                    Optional.of(jdbcColumnTypes.build()),
                    remoteTable);
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
    }

    @Override
    public void finishInsertTable(ConnectorSession session, JdbcOutputTableHandle handle)
    {
    }

    @Override
    public void rollbackCreateTable(ConnectorSession session, JdbcOutputTableHandle handle)
    {
        // avoid delete source table in Ignite
        if (handle.getTableName().equals(handle.getTemporaryTableName())) {
            return;
        }
        dropTable(session, new JdbcTableHandle(
                new SchemaTableName(handle.getSchemaName(), handle.getTemporaryTableName()),
                handle.getCatalogName(),
                handle.getSchemaName(),
                handle.getTemporaryTableName()));
    }

    @Override
    public void renameTable(ConnectorSession session, JdbcTableHandle handle, SchemaTableName newTableName)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support rename table");
    }

    @Override
    protected String createTableSql(RemoteTableName remoteTableName, List<String> columns, ConnectorTableMetadata tableMetadata)
    {
        return createTableSql(remoteTableName, columns, tableMetadata.getProperties());
    }

    private String createTableSql(RemoteTableName remoteTableName, List<String> columns, Map<String, Object> tableProperties)
    {
        ImmutableList.Builder<String> tableOptions = ImmutableList.builder();
        ImmutableList.Builder<String> columnDefinitions = ImmutableList.builder();
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
                case IgniteTableProperties.BACKUPS_PROPERTY:
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
    public boolean supportsTopN(ConnectorSession session, JdbcTableHandle handle, List<JdbcSortItem> sortOrder)
    {
        return true;
    }

    @Override
    protected Optional<TopNFunction> topNFunction()
    {
        return Optional.of(TopNFunction.sqlStandard(this::quoted));
    }

    @Override
    public boolean isTopNGuaranteed(ConnectorSession session)
    {
        return true;
    }

    @Override
    public Map<String, Object> getTableProperties(ConnectorSession session, JdbcTableHandle tableHandle)
    {
        ImmutableMap.Builder<String, Object> properties = ImmutableMap.builder();
        SchemaTableName schemaTableName = tableHandle.asPlainTable().getSchemaTableName();
        String tableName = requireNonNull(schemaTableName.getTableName(), "Ignite table name can not be null").toUpperCase(ENGLISH);
        String sql = format("SELECT idx.CACHE_ID, " +
                "che.CACHE_MODE AS TEMPLATE, " +
                "che.WRITE_SYNCHRONIZATION_MODE, " +
                "che.ATOMICITY_MODE AS ATOMICITY, " +
                "che.DATA_REGION_NAME AS DATA_REGION, " +
                "che.CACHE_GROUP_NAME AS CACHE_GROUP, " +
                "che.CACHE_NAME, " +
                "che.BACKUPS, " +
                "(SELECT COLUMNS FROM sys.indexes WHERE SCHEMA_NAME = 'PUBLIC' AND TABLE_NAME = '%s' AND INDEX_NAME = '_key_PK') AS PKS," +
                "(SELECT COLUMNS FROM sys.indexes WHERE SCHEMA_NAME = 'PUBLIC' AND TABLE_NAME = '%1$s' AND INDEX_NAME = 'AFFINITY_KEY') AS AFK FROM sys.indexes as idx " +
                "JOIN sys.caches che ON idx.CACHE_ID = che.CACHE_ID WHERE idx.SCHEMA_NAME = 'PUBLIC' AND idx.TABLE_NAME = '%1$s' LIMIT 1", tableName);

        try (Connection connection = connectionFactory.openConnection(session);
                PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
            ResultSet resultSet = preparedStatement.executeQuery();
            checkArgument(resultSet.next(), "Ignite table: '" + tableName + "' properties is NULL");
            List<String> primaryKeys = extractColumnNamesFromOrderingScheme(resultSet.getString("PKS"));
            checkArgument(!primaryKeys.isEmpty(), "Ignite table should has at least one primary key");
            properties.put(IgniteTableProperties.PRIMARY_KEY_PROPERTY, primaryKeys);

            for (String property : IgniteTableProperties.WITH_PROPERTIES) {
                switch (property) {
                    case IgniteTableProperties.AFFINITY_KEY_PROPERTY:
                        List<String> affinityKeys = extractColumnNamesFromOrderingScheme(resultSet.getString("AFK"));
                        if (!affinityKeys.isEmpty()) {
                            String affinityKey = affinityKeys.get(0);
                            checkArgument(ImmutableSet.copyOf(primaryKeys).contains(affinityKey), "Table affinity key should be one of the primary key");
                            properties.put(property, affinityKey);
                        }
                        break;
                    case IgniteTableProperties.BACKUPS_PROPERTY:
                        int backups = resultSet.getInt(property.toUpperCase(ENGLISH));
                        // backups should at least greater than 0, but default value is 1, we do not show the default value
                        if (backups > 1) {
                            properties.put(property, backups);
                        }
                        break;
                    case IgniteTableProperties.TEMPLATE_PROPERTY:
                        Optional.ofNullable(resultSet.getString(property.toUpperCase(ENGLISH)))
                                .map(IgniteTemplateType::valueOf)
                                .filter(template -> template != IgniteTemplateType.PARTITIONED)
                                .ifPresent(value -> properties.put(property, value));
                        break;
                    case IgniteTableProperties.WRITE_SYNCHRONIZATION_MODE_PROPERTY:
                        Optional.ofNullable(resultSet.getString(property.toUpperCase(ENGLISH)))
                                .map(IgniteWriteSyncMode::valueOf)
                                .filter(mode -> mode != IgniteWriteSyncMode.FULL_SYNC)
                                .ifPresent(value -> properties.put(property, value));
                        break;
                    case IgniteTableProperties.CACHE_NAME_PROPERTY:
                    case IgniteTableProperties.CACHE_GROUP_PROPERTY:
                        Optional.ofNullable(resultSet.getString(property.toUpperCase(ENGLISH)))
                                .filter(name -> !name.equals("SQL_PUBLIC_" + tableName))
                                .ifPresent(value -> properties.put(property, value));
                        break;
                    case IgniteTableProperties.DATA_REGION_PROPERTY:
                        Optional.ofNullable(resultSet.getString(property.toUpperCase(ENGLISH))).ifPresent(value -> properties.put(property, value));
                        break;
                    default:
                        throw new IllegalStateException("Unexpected value: " + property);
                }
            }
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
        return properties.build();
    }

    // extract result from format: "ID" ASC, "CITY_ID" ASC
    private List<String> extractColumnNamesFromOrderingScheme(String row)
    {
        ImmutableList.Builder<String> builder = ImmutableList.builder();
        if (isNullOrEmpty(row)) {
            return builder.build();
        }
        for (String key : SPLITTER.split(row)) {
            int left = key.indexOf("\"") + 1;
            int right = key.lastIndexOf("\"");
            builder.add(key.substring(left, right).toLowerCase(ENGLISH));
        }
        return builder.build();
    }

    @Override
    public WriteMapping toWriteMapping(ConnectorSession session, Type type)
    {
        if (type instanceof DateType) {
            return WriteMapping.longMapping("date", dateWriteFunction());
        }
        if (type instanceof DecimalType) {
            DecimalType decimalType = (DecimalType) type;
            String dataType = format("decimal(%s, %s)", decimalType.getPrecision(), decimalType.getScale());
            if (decimalType.isShort()) {
                return WriteMapping.longMapping(dataType, shortDecimalWriteFunction(decimalType));
            }
            return WriteMapping.sliceMapping(dataType, longDecimalWriteFunction(decimalType.getPrecision(), decimalType.getScale()));
        }
        // TODO
        return legacyToWriteMapping(session, type);
//        throw new TrinoException(NOT_SUPPORTED, "Unsupported column type: " + type.getDisplayName());
    }

    public static SliceWriteFunction longDecimalWriteFunction(int precision, int scale)
    {
        String bindExpression = format("CAST(? AS decimal(%s, %s))", precision, scale);
        return new SliceWriteFunction()
        {
            @Override
            public String getBindExpression()
            {
                return bindExpression;
            }

            @Override
            public void set(PreparedStatement statement, int index, Slice value)
                    throws SQLException
            {
                statement.setBigDecimal(index, new BigDecimal(value.getLong(0)));
            }
        };
    }

    @Override
    protected void copyTableSchema(Connection connection, String catalogName, String schemaName, String tableName, String newTableName, List<String> columnNames)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support creating tables with data");
    }

    @Override
    public void commitCreateTable(ConnectorSession session, JdbcOutputTableHandle handle)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support creating tables with data");
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
