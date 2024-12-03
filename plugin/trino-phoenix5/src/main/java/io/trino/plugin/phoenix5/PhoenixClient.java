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
package io.trino.plugin.phoenix5;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import io.trino.plugin.base.expression.ConnectorExpressionRewriter;
import io.trino.plugin.base.mapping.IdentifierMapping;
import io.trino.plugin.base.mapping.RemoteIdentifiers;
import io.trino.plugin.jdbc.BaseJdbcClient;
import io.trino.plugin.jdbc.ColumnMapping;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.JdbcColumnHandle;
import io.trino.plugin.jdbc.JdbcOutputTableHandle;
import io.trino.plugin.jdbc.JdbcSortItem;
import io.trino.plugin.jdbc.JdbcSplit;
import io.trino.plugin.jdbc.JdbcTableHandle;
import io.trino.plugin.jdbc.JdbcTypeHandle;
import io.trino.plugin.jdbc.LongReadFunction;
import io.trino.plugin.jdbc.LongWriteFunction;
import io.trino.plugin.jdbc.ObjectReadFunction;
import io.trino.plugin.jdbc.ObjectWriteFunction;
import io.trino.plugin.jdbc.PredicatePushdownController;
import io.trino.plugin.jdbc.PreparedQuery;
import io.trino.plugin.jdbc.QueryBuilder;
import io.trino.plugin.jdbc.RemoteTableName;
import io.trino.plugin.jdbc.WriteFunction;
import io.trino.plugin.jdbc.WriteMapping;
import io.trino.plugin.jdbc.expression.ComparisonOperator;
import io.trino.plugin.jdbc.expression.JdbcConnectorExpressionRewriterBuilder;
import io.trino.plugin.jdbc.expression.ParameterizedExpression;
import io.trino.plugin.jdbc.expression.RewriteComparison;
import io.trino.plugin.jdbc.logging.RemoteQueryModifier;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.JoinStatistics;
import io.trino.spi.connector.JoinType;
import io.trino.spi.connector.SchemaNotFoundException;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Decimals;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.compile.StatementContext;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.iterate.ConcatResultIterator;
import org.apache.phoenix.iterate.LookAheadResultIterator;
import org.apache.phoenix.iterate.MapReduceParallelScanGrouper;
import org.apache.phoenix.iterate.PeekingResultIterator;
import org.apache.phoenix.iterate.ResultIterator;
import org.apache.phoenix.iterate.SequenceResultIterator;
import org.apache.phoenix.iterate.TableResultIterator;
import org.apache.phoenix.jdbc.DelegatePreparedStatement;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixPreparedStatement;
import org.apache.phoenix.jdbc.PhoenixResultSet;
import org.apache.phoenix.mapreduce.PhoenixInputSplit;
import org.apache.phoenix.monitoring.ScanMetricsHolder;
import org.apache.phoenix.query.ConnectionQueryServices;
import org.apache.phoenix.query.HBaseFactoryProvider;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PName;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.TableProperty;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PhoenixArray;
import org.apache.phoenix.util.SchemaUtil;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.StringJoiner;
import java.util.function.BiFunction;

import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.jdbc.DecimalConfig.DecimalMapping.ALLOW_OVERFLOW;
import static io.trino.plugin.jdbc.DecimalSessionSessionProperties.getDecimalDefaultScale;
import static io.trino.plugin.jdbc.DecimalSessionSessionProperties.getDecimalRounding;
import static io.trino.plugin.jdbc.DecimalSessionSessionProperties.getDecimalRoundingMode;
import static io.trino.plugin.jdbc.PredicatePushdownController.DISABLE_PUSHDOWN;
import static io.trino.plugin.jdbc.PredicatePushdownController.FULL_PUSHDOWN;
import static io.trino.plugin.jdbc.StandardColumnMappings.bigintColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.bigintWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.booleanColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.booleanWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.charWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.decimalColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.defaultCharColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.defaultVarcharColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.doubleColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.doubleWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.integerColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.integerWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.longDecimalWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.realColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.realWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.shortDecimalWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.smallintColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.smallintWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.tinyintColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.tinyintWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.varbinaryColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.varbinaryWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.varcharColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.varcharWriteFunction;
import static io.trino.plugin.jdbc.TypeHandlingJdbcSessionProperties.getUnsupportedTypeHandling;
import static io.trino.plugin.jdbc.UnsupportedTypeHandling.CONVERT_TO_VARCHAR;
import static io.trino.plugin.phoenix5.ConfigurationInstantiator.newEmptyConfiguration;
import static io.trino.plugin.phoenix5.MetadataUtil.getEscapedTableName;
import static io.trino.plugin.phoenix5.MetadataUtil.toPhoenixSchemaName;
import static io.trino.plugin.phoenix5.PhoenixClientModule.getConnectionProperties;
import static io.trino.plugin.phoenix5.PhoenixColumnProperties.isPrimaryKey;
import static io.trino.plugin.phoenix5.PhoenixErrorCode.PHOENIX_METADATA_ERROR;
import static io.trino.plugin.phoenix5.PhoenixErrorCode.PHOENIX_QUERY_ERROR;
import static io.trino.plugin.phoenix5.PhoenixMetadata.DEFAULT_SCHEMA;
import static io.trino.plugin.phoenix5.TypeUtils.getArrayElementPhoenixTypeName;
import static io.trino.plugin.phoenix5.TypeUtils.getJdbcObjectArray;
import static io.trino.plugin.phoenix5.TypeUtils.jdbcObjectArrayToBlock;
import static io.trino.plugin.phoenix5.TypeUtils.toBoxedArray;
import static io.trino.spi.StandardErrorCode.ALREADY_EXISTS;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.connector.ConnectorMetadata.MODIFYING_ROWS_MESSAGE;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DecimalType.DEFAULT_PRECISION;
import static io.trino.spi.type.DecimalType.DEFAULT_SCALE;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static java.lang.Math.max;
import static java.lang.String.format;
import static java.lang.String.join;
import static java.math.RoundingMode.UNNECESSARY;
import static java.sql.Types.ARRAY;
import static java.sql.Types.LONGNVARCHAR;
import static java.sql.Types.LONGVARCHAR;
import static java.sql.Types.NVARCHAR;
import static java.sql.Types.TIMESTAMP;
import static java.sql.Types.TIMESTAMP_WITH_TIMEZONE;
import static java.sql.Types.TIME_WITH_TIMEZONE;
import static java.sql.Types.VARCHAR;
import static java.util.Locale.ENGLISH;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toSet;
import static org.apache.hadoop.hbase.HConstants.FOREVER;
import static org.apache.phoenix.coprocessorclient.BaseScannerRegionObserverConstants.SKIP_REGION_BOUNDARY_CHECK;
import static org.apache.phoenix.util.PhoenixRuntime.getTable;
import static org.apache.phoenix.util.SchemaUtil.ESCAPE_CHARACTER;
import static org.apache.phoenix.util.SchemaUtil.getEscapedArgument;

public class PhoenixClient
        extends BaseJdbcClient
{
    public static final String MERGE_ROW_ID_COLUMN_NAME = "$merge_row_id";
    public static final String ROWKEY = "ROWKEY";
    public static final JdbcColumnHandle ROWKEY_COLUMN_HANDLE = new JdbcColumnHandle(
            ROWKEY,
            new JdbcTypeHandle(Types.BIGINT, Optional.of("BIGINT"), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty()),
            BIGINT);

    private static final String DATE_FORMAT = "y-MM-dd G";
    private static final DateTimeFormatter LOCAL_DATE_FORMATTER = DateTimeFormatter.ofPattern(DATE_FORMAT);

    // Phoenix threshold for simplifying big IN predicates is 50k https://issues.apache.org/jira/browse/PHOENIX-6751
    public static final int DEFAULT_DOMAIN_COMPACTION_THRESHOLD = 5_000;

    private final Configuration configuration;

    private final ConnectorExpressionRewriter<ParameterizedExpression> connectorExpressionRewriter;

    @Inject
    public PhoenixClient(PhoenixConfig config, ConnectionFactory connectionFactory, QueryBuilder queryBuilder, IdentifierMapping identifierMapping, RemoteQueryModifier queryModifier)
            throws SQLException
    {
        super(
                ESCAPE_CHARACTER,
                connectionFactory,
                queryBuilder,
                ImmutableSet.of(),
                identifierMapping,
                queryModifier,
                false);
        this.configuration = newEmptyConfiguration();
        getConnectionProperties(config).forEach((k, v) -> configuration.set((String) k, (String) v));
        this.connectorExpressionRewriter = JdbcConnectorExpressionRewriterBuilder.newBuilder()
                .addStandardRules(this::quoted)
                .add(new RewriteComparison(ImmutableSet.of(ComparisonOperator.EQUAL, ComparisonOperator.NOT_EQUAL)))
                .withTypeClass("integer_type", ImmutableSet.of("tinyint", "smallint", "integer", "bigint"))
                .map("$add(left: integer_type, right: integer_type)").to("left + right")
                .map("$subtract(left: integer_type, right: integer_type)").to("left - right")
                .map("$multiply(left: integer_type, right: integer_type)").to("left * right")
                .map("$divide(left: integer_type, right: integer_type)").to("left / right")
                .map("$modulus(left: integer_type, right: integer_type)").to("left % right")
                .map("$negate(value: integer_type)").to("-value")
                .build();
    }

    @Override
    public Optional<ParameterizedExpression> convertPredicate(ConnectorSession session, ConnectorExpression expression, Map<String, ColumnHandle> assignments)
    {
        return connectorExpressionRewriter.rewrite(session, expression, assignments);
    }

    @Override
    public Optional<PreparedQuery> implementJoin(
            ConnectorSession session,
            JoinType joinType,
            PreparedQuery leftSource,
            Map<JdbcColumnHandle, String> leftProjections,
            PreparedQuery rightSource,
            Map<JdbcColumnHandle, String> rightProjections,
            List<ParameterizedExpression> joinConditions,
            JoinStatistics statistics)
    {
        // Joins are currently not supported
        return Optional.empty();
    }

    @Override
    public Connection getConnection(ConnectorSession session)
            throws SQLException
    {
        return connectionFactory.openConnection(session);
    }

    public org.apache.hadoop.hbase.client.Connection getHConnection()
            throws IOException
    {
        return HBaseFactoryProvider.getHConnectionFactory().createConnection(configuration);
    }

    @Override
    public void execute(ConnectorSession session, String statement)
    {
        super.execute(session, statement);
    }

    @Override
    public Collection<String> listSchemas(Connection connection)
    {
        try (ResultSet resultSet = connection.getMetaData().getSchemas()) {
            ImmutableSet.Builder<String> schemaNames = ImmutableSet.builder();
            schemaNames.add(DEFAULT_SCHEMA);
            while (resultSet.next()) {
                String schemaName = getTableSchemaName(resultSet);
                // skip internal schemas
                if (filterSchema(schemaName)) {
                    schemaNames.add(schemaName);
                }
            }
            return schemaNames.build();
        }
        catch (SQLException e) {
            throw new TrinoException(PHOENIX_METADATA_ERROR, e);
        }
    }

    @Override
    public PreparedStatement buildSql(ConnectorSession session, Connection connection, JdbcSplit split, JdbcTableHandle table, List<JdbcColumnHandle> columnHandles)
            throws SQLException
    {
        PreparedStatement query = prepareStatement(
                session,
                connection,
                table,
                columnHandles,
                Optional.of(split));
        QueryPlan queryPlan = getQueryPlan(query.unwrap(PhoenixPreparedStatement.class));
        ResultSet resultSet = getResultSet(((PhoenixSplit) split).getPhoenixInputSplit(), queryPlan);
        return new DelegatePreparedStatement(query)
        {
            @Override
            public ResultSet executeQuery()
            {
                return resultSet;
            }
        };
    }

    public PreparedStatement prepareStatement(
            ConnectorSession session,
            Connection connection,
            JdbcTableHandle table,
            List<JdbcColumnHandle> columns,
            Optional<JdbcSplit> split)
            throws SQLException
    {
        PreparedQuery preparedQuery = prepareQuery(
                session,
                connection,
                table,
                Optional.empty(),
                columns,
                ImmutableMap.of(),
                split);
        return queryBuilder.prepareStatement(this, session, connection, preparedQuery, Optional.of(columns.size()));
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
        // There are multiple splits and TopN is not guaranteed across them.
        return false;
    }

    @Override
    public OptionalLong update(ConnectorSession session, JdbcTableHandle handle)
    {
        throw new TrinoException(NOT_SUPPORTED, MODIFYING_ROWS_MESSAGE);
    }

    @Override
    protected Optional<BiFunction<String, Long, String>> limitFunction()
    {
        return Optional.of((sql, limit) -> sql + " LIMIT " + limit);
    }

    @Override
    public boolean isLimitGuaranteed(ConnectorSession session)
    {
        return false;
    }

    @Override
    public String buildInsertSql(JdbcOutputTableHandle handle, List<WriteFunction> columnWriters)
    {
        PhoenixOutputTableHandle outputHandle = (PhoenixOutputTableHandle) handle;
        String params = columnWriters.stream()
                .map(WriteFunction::getBindExpression)
                .collect(joining(","));
        String columns = handle.getColumnNames().stream()
                .map(SchemaUtil::getEscapedArgument)
                .collect(joining(","));
        if (outputHandle.rowkeyColumn().isPresent()) {
            String nextId = format(
                    "NEXT VALUE FOR %s, ",
                    quoted(null, handle.getRemoteTableName().getSchemaName().orElse(null), handle.getRemoteTableName().getTableName() + "_sequence"));
            params = nextId + params;
            columns = outputHandle.rowkeyColumn().get() + ", " + columns;
        }
        return format(
                "UPSERT INTO %s (%s) VALUES (%s)",
                quoted(handle.getRemoteTableName()),
                columns,
                params);
    }

    @Override
    public ResultSet getTables(Connection connection, Optional<String> schemaName, Optional<String> tableName)
            throws SQLException
    {
        return super.getTables(connection, schemaName.map(MetadataUtil::toPhoenixSchemaName), tableName);
    }

    @Override
    protected String getTableSchemaName(ResultSet resultSet)
            throws SQLException
    {
        return firstNonNull(resultSet.getString("TABLE_SCHEM"), DEFAULT_SCHEMA);
    }

    @Override
    protected ResultSet getColumns(RemoteTableName remoteTableName, DatabaseMetaData metadata)
            throws SQLException
    {
        try {
            return super.getColumns(remoteTableName, metadata);
        }
        catch (org.apache.phoenix.schema.TableNotFoundException e) {
            // Most JDBC driver return an empty result when DatabaseMetaData.getColumns can't find objects, but Phoenix driver throws an exception
            // Rethrow as Trino TableNotFoundException to suppress the exception during listing information_schema
            throw new io.trino.spi.connector.TableNotFoundException(new SchemaTableName(remoteTableName.getSchemaName().orElse(null), remoteTableName.getTableName()));
        }
    }

    @Override
    public Optional<ColumnMapping> toColumnMapping(ConnectorSession session, Connection connection, JdbcTypeHandle typeHandle)
    {
        Optional<ColumnMapping> mapping = getForcedMappingToVarchar(typeHandle);
        if (mapping.isPresent()) {
            return mapping;
        }

        switch (typeHandle.jdbcType()) {
            case Types.BOOLEAN:
                return Optional.of(booleanColumnMapping());

            case Types.TINYINT:
                return Optional.of(tinyintColumnMapping());

            case Types.SMALLINT:
                return Optional.of(smallintColumnMapping());

            case Types.INTEGER:
                return Optional.of(integerColumnMapping());

            case Types.BIGINT:
                return Optional.of(bigintColumnMapping());

            case Types.FLOAT:
                return Optional.of(realColumnMapping());

            case Types.DOUBLE:
                return Optional.of(doubleColumnMapping());

            case Types.DECIMAL:
                Optional<Integer> columnSize = typeHandle.columnSize();
                int precision = columnSize.orElse(DEFAULT_PRECISION);
                int decimalDigits = typeHandle.decimalDigits().orElse(DEFAULT_SCALE);
                if (getDecimalRounding(session) == ALLOW_OVERFLOW) {
                    if (columnSize.isEmpty()) {
                        return Optional.of(decimalColumnMapping(createDecimalType(Decimals.MAX_PRECISION, getDecimalDefaultScale(session)), getDecimalRoundingMode(session)));
                    }
                }
                // TODO does phoenix support negative scale?
                precision = precision + max(-decimalDigits, 0); // Map decimal(p, -s) (negative scale) to decimal(p+s, 0).
                if (precision > Decimals.MAX_PRECISION) {
                    break;
                }
                return Optional.of(decimalColumnMapping(createDecimalType(precision, max(decimalDigits, 0)), UNNECESSARY));

            case Types.CHAR:
                return Optional.of(defaultCharColumnMapping(typeHandle.requiredColumnSize(), true));

            case VARCHAR:
            case NVARCHAR:
            case LONGVARCHAR:
            case LONGNVARCHAR:
                if (typeHandle.columnSize().isEmpty()) {
                    return Optional.of(varcharColumnMapping(createUnboundedVarcharType(), true));
                }
                return Optional.of(defaultVarcharColumnMapping(typeHandle.requiredColumnSize(), true));

            case Types.BINARY:
            case Types.VARBINARY:
                return Optional.of(varbinaryColumnMapping());

            case Types.DATE:
                return Optional.of(ColumnMapping.longMapping(
                        DATE,
                        dateReadFunction(),
                        dateWriteFunctionUsingString()));

            // TODO add support for TIMESTAMP after Phoenix adds support for LocalDateTime
            case TIMESTAMP:
            case TIME_WITH_TIMEZONE:
            case TIMESTAMP_WITH_TIMEZONE:
                if (getUnsupportedTypeHandling(session) == CONVERT_TO_VARCHAR) {
                    return mapToUnboundedVarchar(typeHandle);
                }
                return Optional.empty();

            case ARRAY:
                JdbcTypeHandle elementTypeHandle = getArrayElementTypeHandle(typeHandle);
                if (elementTypeHandle.jdbcType() == Types.VARBINARY) {
                    return Optional.empty();
                }
                return toColumnMapping(session, connection, elementTypeHandle)
                        .map(elementMapping -> {
                            ArrayType trinoArrayType = new ArrayType(elementMapping.getType());
                            String jdbcTypeName = elementTypeHandle.jdbcTypeName()
                                    .orElseThrow(() -> new TrinoException(
                                            PHOENIX_METADATA_ERROR,
                                            "Type name is missing for jdbc type: " + JDBCType.valueOf(elementTypeHandle.jdbcType())));
                            // TODO (https://github.com/trinodb/trino/issues/11132) Enable predicate pushdown on ARRAY(CHAR) type in Phoenix
                            PredicatePushdownController pushdownController = elementTypeHandle.jdbcType() == Types.CHAR ? DISABLE_PUSHDOWN : FULL_PUSHDOWN;
                            return arrayColumnMapping(session, trinoArrayType, jdbcTypeName, pushdownController);
                        });
        }
        if (getUnsupportedTypeHandling(session) == CONVERT_TO_VARCHAR) {
            return mapToUnboundedVarchar(typeHandle);
        }
        return Optional.empty();
    }

    @Override
    public WriteMapping toWriteMapping(ConnectorSession session, Type type)
    {
        if (type == BOOLEAN) {
            return WriteMapping.booleanMapping("boolean", booleanWriteFunction());
        }

        if (type == TINYINT) {
            return WriteMapping.longMapping("tinyint", tinyintWriteFunction());
        }
        if (type == SMALLINT) {
            return WriteMapping.longMapping("smallint", smallintWriteFunction());
        }
        if (type == INTEGER) {
            return WriteMapping.longMapping("integer", integerWriteFunction());
        }
        if (type == BIGINT) {
            return WriteMapping.longMapping("bigint", bigintWriteFunction());
        }
        if (type == REAL) {
            return WriteMapping.longMapping("float", realWriteFunction());
        }
        if (type == DOUBLE) {
            return WriteMapping.doubleMapping("double", doubleWriteFunction());
        }

        if (type instanceof DecimalType decimalType) {
            String dataType = format("decimal(%s, %s)", decimalType.getPrecision(), decimalType.getScale());
            if (decimalType.isShort()) {
                return WriteMapping.longMapping(dataType, shortDecimalWriteFunction(decimalType));
            }
            return WriteMapping.objectMapping(dataType, longDecimalWriteFunction(decimalType));
        }

        if (type instanceof CharType charType) {
            return WriteMapping.sliceMapping("char(" + charType.getLength() + ")", charWriteFunction());
        }
        if (type instanceof VarcharType varcharType) {
            String dataType;
            if (varcharType.isUnbounded()) {
                dataType = "varchar";
            }
            else {
                dataType = "varchar(" + varcharType.getBoundedLength() + ")";
            }
            return WriteMapping.sliceMapping(dataType, varcharWriteFunction());
        }
        if (type instanceof VarbinaryType) {
            return WriteMapping.sliceMapping("varbinary", varbinaryWriteFunction());
        }

        if (type == DATE) {
            return WriteMapping.longMapping("date", dateWriteFunctionUsingString());
        }
        if (type instanceof ArrayType arrayType) {
            Type elementType = arrayType.getElementType();
            String elementDataType = toWriteMapping(session, elementType).getDataType().toUpperCase(ENGLISH);
            String elementWriteName = getArrayElementPhoenixTypeName(session, this, elementType);
            return WriteMapping.objectMapping(elementDataType + " ARRAY", arrayWriteFunction(session, elementType, elementWriteName));
        }
        throw new TrinoException(NOT_SUPPORTED, "Unsupported column type: " + type.getDisplayName());
    }

    @Override
    public Optional<String> getTableComment(ResultSet resultSet)
    {
        // Don't return a comment until the connector supports creating tables with comment
        return Optional.empty();
    }

    @Override
    public JdbcOutputTableHandle beginCreateTable(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        if (tableMetadata.getComment().isPresent()) {
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support creating tables with table comment");
        }
        SchemaTableName schemaTableName = tableMetadata.getTable();
        String schema = schemaTableName.getSchemaName();
        String table = schemaTableName.getTableName();

        if (!getSchemaNames(session).contains(schema)) {
            throw new SchemaNotFoundException(schema);
        }

        try (Connection connection = connectionFactory.openConnection(session)) {
            ConnectorIdentity identity = session.getIdentity();
            RemoteIdentifiers remoteIdentifiers = getRemoteIdentifiers(connection);
            schema = getIdentifierMapping().toRemoteSchemaName(remoteIdentifiers, identity, schema);
            table = getIdentifierMapping().toRemoteTableName(remoteIdentifiers, identity, schema, table);
            schema = toPhoenixSchemaName(schema);
            LinkedList<ColumnMetadata> tableColumns = new LinkedList<>(tableMetadata.getColumns());
            Map<String, Object> tableProperties = tableMetadata.getProperties();
            Optional<Boolean> immutableRows = PhoenixTableProperties.getImmutableRows(tableProperties);
            String immutable = immutableRows.isPresent() && immutableRows.get() ? "IMMUTABLE" : "";

            ImmutableList.Builder<String> columnNames = ImmutableList.builder();
            ImmutableList.Builder<Type> columnTypes = ImmutableList.builder();
            ImmutableList.Builder<String> columnList = ImmutableList.builder();
            Set<ColumnMetadata> rowkeyColumns = tableColumns.stream().filter(col -> isPrimaryKey(col, tableProperties)).collect(toSet());
            ImmutableList.Builder<String> pkNames = ImmutableList.builder();
            Optional<String> rowkeyColumn = Optional.empty();
            if (rowkeyColumns.isEmpty()) {
                // Add a rowkey when not specified in DDL
                columnList.add(ROWKEY + " bigint not null");
                pkNames.add(ROWKEY);
                execute(session, format("CREATE SEQUENCE %s", getEscapedTableName(schema, table + "_sequence")));
                rowkeyColumn = Optional.of(ROWKEY);
            }
            for (ColumnMetadata column : tableColumns) {
                if (column.getComment() != null) {
                    throw new TrinoException(NOT_SUPPORTED, "This connector does not support creating tables with column comment");
                }
                String columnName = getIdentifierMapping().toRemoteColumnName(remoteIdentifiers, column.getName());
                columnNames.add(columnName);
                columnTypes.add(column.getType());
                String typeStatement = toWriteMapping(session, column.getType()).getDataType();
                if (rowkeyColumns.contains(column)) {
                    typeStatement += " not null";
                    pkNames.add(columnName);
                }
                columnList.add(format("%s %s", getEscapedArgument(columnName), typeStatement));
            }

            ImmutableList.Builder<String> tableOptions = ImmutableList.builder();
            PhoenixTableProperties.getSaltBuckets(tableProperties).ifPresent(value -> tableOptions.add(TableProperty.SALT_BUCKETS + "=" + value));
            PhoenixTableProperties.getSplitOn(tableProperties).ifPresent(value -> tableOptions.add("SPLIT ON (" + value.replace('"', '\'') + ")"));
            PhoenixTableProperties.getDisableWal(tableProperties).ifPresent(value -> tableOptions.add(TableProperty.DISABLE_WAL + "=" + value));
            PhoenixTableProperties.getDefaultColumnFamily(tableProperties).ifPresent(value -> tableOptions.add(TableProperty.DEFAULT_COLUMN_FAMILY + "=" + value));
            PhoenixTableProperties.getBloomfilter(tableProperties).ifPresent(value -> tableOptions.add(ColumnFamilyDescriptorBuilder.BLOOMFILTER + "='" + value + "'"));
            PhoenixTableProperties.getVersions(tableProperties).ifPresent(value -> tableOptions.add(HConstants.VERSIONS + "=" + value));
            PhoenixTableProperties.getMinVersions(tableProperties).ifPresent(value -> tableOptions.add(ColumnFamilyDescriptorBuilder.MIN_VERSIONS + "=" + value));
            PhoenixTableProperties.getCompression(tableProperties).ifPresent(value -> tableOptions.add(ColumnFamilyDescriptorBuilder.COMPRESSION + "='" + value + "'"));
            PhoenixTableProperties.getTimeToLive(tableProperties).ifPresent(value -> tableOptions.add(ColumnFamilyDescriptorBuilder.TTL + "=" + value));
            PhoenixTableProperties.getDataBlockEncoding(tableProperties).ifPresent(value -> tableOptions.add(ColumnFamilyDescriptorBuilder.DATA_BLOCK_ENCODING + "='" + value + "'"));

            String sql = format(
                    "CREATE %s TABLE %s (%s , CONSTRAINT PK PRIMARY KEY (%s)) %s",
                    immutable,
                    getEscapedTableName(schema, table),
                    join(", ", columnList.build()),
                    join(", ", pkNames.build()),
                    join(", ", tableOptions.build()));

            execute(session, sql);

            return new PhoenixOutputTableHandle(
                    new RemoteTableName(Optional.empty(), Optional.ofNullable(schema), table),
                    columnNames.build(),
                    columnTypes.build(),
                    Optional.empty(),
                    rowkeyColumn);
        }
        catch (SQLException e) {
            if (e.getErrorCode() == SQLExceptionCode.TABLE_ALREADY_EXIST.getErrorCode()) {
                throw new TrinoException(ALREADY_EXISTS, "Phoenix table already exists", e);
            }
            throw new TrinoException(PHOENIX_METADATA_ERROR, "Error creating Phoenix table", e);
        }
    }

    @Override
    public void renameColumn(ConnectorSession session, JdbcTableHandle handle, JdbcColumnHandle jdbcColumn, String newColumnName)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support renaming columns");
    }

    @Override
    protected void renameTable(ConnectorSession session, String catalogName, String schemaName, String tableName, SchemaTableName newTable)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support renaming tables");
    }

    @Override
    public void renameSchema(ConnectorSession session, String schemaName, String newSchemaName)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support renaming schemas");
    }

    @Override
    public Map<String, Object> getTableProperties(ConnectorSession session, JdbcTableHandle handle)
    {
        ImmutableMap.Builder<String, Object> properties = ImmutableMap.builder();
        RemoteTableName remoteTableName = handle.getRequiredNamedRelation().getRemoteTableName();

        try (Connection connection = connectionFactory.openConnection(session);
                Admin admin = connection.unwrap(PhoenixConnection.class).getQueryServices().getAdmin()) {
            String schemaName = toPhoenixSchemaName(remoteTableName.getSchemaName().orElse(null));
            PTable table = getTable(connection, SchemaUtil.getTableName(schemaName, remoteTableName.getTableName()));

            boolean salted = table.getBucketNum() != null;
            StringJoiner joiner = new StringJoiner(",");
            List<PColumn> pkColumns = table.getPKColumns();
            for (PColumn pkColumn : pkColumns.subList(salted ? 1 : 0, pkColumns.size())) {
                joiner.add(pkColumn.getName().getString());
            }
            properties.put(PhoenixTableProperties.ROWKEYS, joiner.toString());

            if (table.getBucketNum() != null) {
                properties.put(PhoenixTableProperties.SALT_BUCKETS, table.getBucketNum());
            }
            if (table.isWALDisabled()) {
                properties.put(PhoenixTableProperties.DISABLE_WAL, table.isWALDisabled());
            }
            if (table.isImmutableRows()) {
                properties.put(PhoenixTableProperties.IMMUTABLE_ROWS, table.isImmutableRows());
            }

            String defaultFamilyName = QueryConstants.DEFAULT_COLUMN_FAMILY;
            if (table.getDefaultFamilyName() != null) {
                defaultFamilyName = table.getDefaultFamilyName().getString();
                properties.put(PhoenixTableProperties.DEFAULT_COLUMN_FAMILY, defaultFamilyName);
            }

            TableDescriptor tableDesc = admin.getDescriptor(TableName.valueOf(table.getPhysicalName().getBytes()));

            ColumnFamilyDescriptor[] columnFamilies = tableDesc.getColumnFamilies();
            for (ColumnFamilyDescriptor columnFamily : columnFamilies) {
                if (columnFamily.getNameAsString().equals(defaultFamilyName)) {
                    if (columnFamily.getBloomFilterType() != BloomType.NONE) {
                        properties.put(PhoenixTableProperties.BLOOMFILTER, columnFamily.getBloomFilterType());
                    }
                    if (columnFamily.getMaxVersions() != 1) {
                        properties.put(PhoenixTableProperties.VERSIONS, columnFamily.getMaxVersions());
                    }
                    if (columnFamily.getMinVersions() > 0) {
                        properties.put(PhoenixTableProperties.MIN_VERSIONS, columnFamily.getMinVersions());
                    }
                    if (columnFamily.getCompressionType() != Compression.Algorithm.NONE) {
                        properties.put(PhoenixTableProperties.COMPRESSION, columnFamily.getCompressionType());
                    }
                    if (columnFamily.getTimeToLive() < FOREVER) {
                        properties.put(PhoenixTableProperties.TTL, columnFamily.getTimeToLive());
                    }
                    if (columnFamily.getDataBlockEncoding() != DataBlockEncoding.NONE) {
                        properties.put(PhoenixTableProperties.DATA_BLOCK_ENCODING, columnFamily.getDataBlockEncoding());
                    }
                    break;
                }
            }
        }
        catch (org.apache.phoenix.schema.TableNotFoundException e) {
            // Rethrow as Trino TableNotFoundException to suppress the exception during listing information_schema
            throw new io.trino.spi.connector.TableNotFoundException(new SchemaTableName(remoteTableName.getSchemaName().orElse(null), remoteTableName.getTableName()));
        }
        catch (IOException | SQLException e) {
            throw new TrinoException(PHOENIX_METADATA_ERROR, "Couldn't get Phoenix table properties", e);
        }
        return properties.buildOrThrow();
    }

    @Override
    public void setColumnType(ConnectorSession session, JdbcTableHandle handle, JdbcColumnHandle column, Type type)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support setting column types");
    }

    @Override
    public List<JdbcColumnHandle> getPrimaryKeys(ConnectorSession session, RemoteTableName remoteTableName)
    {
        JdbcTableHandle plainTable = new JdbcTableHandle(remoteTableName.getSchemaTableName(), remoteTableName, Optional.empty());
        Map<String, Object> tableProperties = getTableProperties(session, plainTable);
        List<JdbcColumnHandle> primaryKeys = getColumns(session, remoteTableName.getSchemaTableName(), remoteTableName)
                .stream()
                .filter(columnHandle -> PhoenixColumnProperties.isPrimaryKey(columnHandle.getColumnMetadata(), tableProperties))
                .collect(toImmutableList());
        verify(!primaryKeys.isEmpty(), "Phoenix primary keys is empty");
        return primaryKeys;
    }

    private static LongReadFunction dateReadFunction()
    {
        return (resultSet, index) -> {
            // Convert to LocalDate from java.sql.Date via String because java.sql.Date#toLocalDate() returns wrong results in B.C. dates. -5881579-07-11 -> +5881580-07-11
            // Phoenix JDBC driver supports getObject(index, LocalDate.class), but it leads to incorrect issues. -5877641-06-23 -> 7642-06-23 & 5881580-07-11 -> 1580-07-11
            // The current implementation still returns +10 days during julian -> gregorian switch
            return LocalDate.parse(new SimpleDateFormat(DATE_FORMAT).format(resultSet.getDate(index)), LOCAL_DATE_FORMATTER).toEpochDay();
        };
    }

    private static LongWriteFunction dateWriteFunctionUsingString()
    {
        return new LongWriteFunction() {
            @Override
            public String getBindExpression()
            {
                return "TO_DATE(?, 'y-MM-dd G', 'local')";
            }

            @Override
            public void set(PreparedStatement statement, int index, long value)
                    throws SQLException
            {
                statement.setString(index, LOCAL_DATE_FORMATTER.format(LocalDate.ofEpochDay(value)));
            }
        };
    }

    private static ColumnMapping arrayColumnMapping(ConnectorSession session, ArrayType arrayType, String elementJdbcTypeName, PredicatePushdownController pushdownController)
    {
        return ColumnMapping.objectMapping(
                arrayType,
                arrayReadFunction(session, arrayType.getElementType()),
                arrayWriteFunction(session, arrayType.getElementType(), elementJdbcTypeName),
                pushdownController);
    }

    private static ObjectReadFunction arrayReadFunction(ConnectorSession session, Type elementType)
    {
        return ObjectReadFunction.of(Block.class, (resultSet, columnIndex) -> {
            Object[] objectArray = toBoxedArray(resultSet.getArray(columnIndex).getArray());
            return jdbcObjectArrayToBlock(session, elementType, objectArray);
        });
    }

    private static ObjectWriteFunction arrayWriteFunction(ConnectorSession session, Type elementType, String elementJdbcTypeName)
    {
        return ObjectWriteFunction.of(Block.class, (statement, index, block) -> {
            Object[] jdbcObjectArray = getJdbcObjectArray(session, elementType, block);
            PhoenixArray phoenixArray = (PhoenixArray) statement.getConnection().createArrayOf(elementJdbcTypeName, jdbcObjectArray);
            for (int i = 0; i < jdbcObjectArray.length; i++) {
                if (jdbcObjectArray[i] == null && phoenixArray.getElement(i) != null) {
                    // TODO (https://github.com/trinodb/trino/issues/6421) Prevent writing incorrect results due to Phoenix JDBC driver bug
                    throw new TrinoException(PHOENIX_QUERY_ERROR, format("Phoenix JDBC driver replaced 'null' with '%s' at index %s in %s", phoenixArray.getElement(i), i + 1, phoenixArray));
                }
            }
            statement.setArray(index, phoenixArray);
        });
    }

    private JdbcTypeHandle getArrayElementTypeHandle(JdbcTypeHandle arrayTypeHandle)
    {
        String arrayTypeName = arrayTypeHandle.jdbcTypeName()
                .orElseThrow(() -> new TrinoException(PHOENIX_METADATA_ERROR, "Type name is missing for jdbc type: " + JDBCType.valueOf(arrayTypeHandle.jdbcType())));
        checkArgument(arrayTypeName.endsWith(" ARRAY"), "array type must end with ' ARRAY'");
        arrayTypeName = arrayTypeName.substring(0, arrayTypeName.length() - " ARRAY".length());
        verify(arrayTypeHandle.caseSensitivity().isEmpty(), "Case sensitivity not supported");
        return new JdbcTypeHandle(
                PDataType.fromSqlTypeName(arrayTypeName).getSqlType(),
                Optional.of(arrayTypeName),
                arrayTypeHandle.columnSize(),
                arrayTypeHandle.decimalDigits(),
                Optional.empty(),
                Optional.empty());
    }

    public QueryPlan getQueryPlan(PhoenixPreparedStatement inputQuery)
    {
        try {
            // Optimize the query plan so that we potentially use secondary indexes
            QueryPlan queryPlan = inputQuery.optimizeQuery();
            // Initialize the query plan so it sets up the parallel scans
            queryPlan.iterator(MapReduceParallelScanGrouper.getInstance());
            return queryPlan;
        }
        catch (SQLException e) {
            throw new TrinoException(PHOENIX_QUERY_ERROR, "Failed to get the Phoenix query plan", e);
        }
    }

    private static ResultSet getResultSet(PhoenixInputSplit split, QueryPlan queryPlan)
    {
        List<Scan> scans = split.getScans();
        try {
            List<PeekingResultIterator> iterators = new ArrayList<>(scans.size());
            StatementContext context = queryPlan.getContext();
            // Clear the table region boundary cache to make sure long running jobs stay up to date
            PName physicalTableName = queryPlan.getTableRef().getTable().getPhysicalName();
            PhoenixConnection phoenixConnection = context.getConnection();
            ConnectionQueryServices services = phoenixConnection.getQueryServices();
            services.clearTableRegionCache(TableName.valueOf(physicalTableName.getBytes()));

            for (Scan scan : scans) {
                scan = new Scan(scan);
                // For MR, skip the region boundary check exception if we encounter a split. ref: PHOENIX-2599
                scan.setAttribute(SKIP_REGION_BOUNDARY_CHECK, Bytes.toBytes(true));

                ScanMetricsHolder scanMetricsHolder = ScanMetricsHolder.getInstance(
                        context.getReadMetricsQueue(),
                        physicalTableName.getString(),
                        scan,
                        phoenixConnection.getLogLevel());

                TableResultIterator tableResultIterator = new TableResultIterator(
                        phoenixConnection.getMutationState(),
                        scan,
                        scanMetricsHolder,
                        services.getRenewLeaseThresholdMilliSeconds(),
                        queryPlan,
                        MapReduceParallelScanGrouper.getInstance());
                iterators.add(LookAheadResultIterator.wrap(tableResultIterator));
            }
            ResultIterator iterator = ConcatResultIterator.newIterator(iterators);
            if (context.getSequenceManager().getSequenceCount() > 0) {
                iterator = new SequenceResultIterator(iterator, context.getSequenceManager());
            }
            // Clone the row projector as it's not thread safe and would be used simultaneously by
            // multiple threads otherwise.
            return new PhoenixResultSet(iterator, queryPlan.getProjector().cloneIfNecessary(), context);
        }
        catch (SQLException e) {
            throw new TrinoException(PHOENIX_QUERY_ERROR, "Error while setting up Phoenix ResultSet", e);
        }
        catch (IOException e) {
            throw new TrinoException(PhoenixErrorCode.PHOENIX_INTERNAL_ERROR, "Error while copying scan", e);
        }
    }
}
