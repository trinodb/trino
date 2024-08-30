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
package io.trino.plugin.oracle;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.trino.plugin.base.aggregation.AggregateFunctionRewriter;
import io.trino.plugin.base.aggregation.AggregateFunctionRule;
import io.trino.plugin.base.expression.ConnectorExpressionRewriter;
import io.trino.plugin.base.mapping.IdentifierMapping;
import io.trino.plugin.jdbc.BaseJdbcClient;
import io.trino.plugin.jdbc.BaseJdbcConfig;
import io.trino.plugin.jdbc.BooleanWriteFunction;
import io.trino.plugin.jdbc.ColumnMapping;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.DoubleWriteFunction;
import io.trino.plugin.jdbc.JdbcColumnHandle;
import io.trino.plugin.jdbc.JdbcExpression;
import io.trino.plugin.jdbc.JdbcJoinCondition;
import io.trino.plugin.jdbc.JdbcNamedRelationHandle;
import io.trino.plugin.jdbc.JdbcQueryRelationHandle;
import io.trino.plugin.jdbc.JdbcRelationHandle;
import io.trino.plugin.jdbc.JdbcSplit;
import io.trino.plugin.jdbc.JdbcTableHandle;
import io.trino.plugin.jdbc.JdbcTypeHandle;
import io.trino.plugin.jdbc.LongReadFunction;
import io.trino.plugin.jdbc.LongWriteFunction;
import io.trino.plugin.jdbc.ObjectReadFunction;
import io.trino.plugin.jdbc.ObjectWriteFunction;
import io.trino.plugin.jdbc.PreparedQuery;
import io.trino.plugin.jdbc.QueryBuilder;
import io.trino.plugin.jdbc.RemoteTableName;
import io.trino.plugin.jdbc.SliceWriteFunction;
import io.trino.plugin.jdbc.WriteMapping;
import io.trino.plugin.jdbc.aggregation.ImplementAvgDecimal;
import io.trino.plugin.jdbc.aggregation.ImplementAvgFloatingPoint;
import io.trino.plugin.jdbc.aggregation.ImplementCount;
import io.trino.plugin.jdbc.aggregation.ImplementCountAll;
import io.trino.plugin.jdbc.aggregation.ImplementCountDistinct;
import io.trino.plugin.jdbc.aggregation.ImplementCovariancePop;
import io.trino.plugin.jdbc.aggregation.ImplementCovarianceSamp;
import io.trino.plugin.jdbc.aggregation.ImplementMinMax;
import io.trino.plugin.jdbc.aggregation.ImplementStddevPop;
import io.trino.plugin.jdbc.aggregation.ImplementStddevSamp;
import io.trino.plugin.jdbc.aggregation.ImplementSum;
import io.trino.plugin.jdbc.aggregation.ImplementVariancePop;
import io.trino.plugin.jdbc.aggregation.ImplementVarianceSamp;
import io.trino.plugin.jdbc.expression.JdbcConnectorExpressionRewriterBuilder;
import io.trino.plugin.jdbc.expression.ParameterizedExpression;
import io.trino.plugin.jdbc.logging.RemoteQueryModifier;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.AggregateFunction;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.FixedSplitSource;
import io.trino.spi.connector.JoinCondition;
import io.trino.spi.connector.JoinStatistics;
import io.trino.spi.connector.JoinType;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Decimals;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import oracle.jdbc.OraclePreparedStatement;
import oracle.jdbc.OracleTypes;

import java.math.RoundingMode;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.WeakHashMap;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.emptyToNull;
import static com.google.common.base.Verify.verify;
import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.slice.Slices.wrappedBuffer;
import static io.trino.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static io.trino.plugin.jdbc.PredicatePushdownController.DISABLE_PUSHDOWN;
import static io.trino.plugin.jdbc.PredicatePushdownController.FULL_PUSHDOWN;
import static io.trino.plugin.jdbc.StandardColumnMappings.bigintColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.bigintWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.charReadFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.fromLongTrinoTimestamp;
import static io.trino.plugin.jdbc.StandardColumnMappings.fromTrinoTimestamp;
import static io.trino.plugin.jdbc.StandardColumnMappings.integerWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.longDecimalReadFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.longDecimalWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.shortDecimalReadFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.shortDecimalWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.smallintWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.tinyintWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.toLongTrinoTimestamp;
import static io.trino.plugin.jdbc.StandardColumnMappings.toTrinoTimestamp;
import static io.trino.plugin.jdbc.StandardColumnMappings.varbinaryWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.varcharWriteFunction;
import static io.trino.plugin.jdbc.TypeHandlingJdbcSessionProperties.getUnsupportedTypeHandling;
import static io.trino.plugin.jdbc.UnsupportedTypeHandling.CONVERT_TO_VARCHAR;
import static io.trino.plugin.oracle.OracleSessionProperties.getNumberDefaultScale;
import static io.trino.plugin.oracle.OracleSessionProperties.getNumberRoundingMode;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.CharType.createCharType;
import static io.trino.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.trino.spi.type.DateTimeEncoding.unpackMillisUtc;
import static io.trino.spi.type.DateTimeEncoding.unpackZoneKey;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimestampType.MAX_SHORT_PRECISION;
import static io.trino.spi.type.TimestampType.TIMESTAMP_SECONDS;
import static io.trino.spi.type.TimestampType.createTimestampType;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS;
import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_SECOND;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static java.lang.Float.floatToRawIntBits;
import static java.lang.Float.intBitsToFloat;
import static java.lang.Math.floorDiv;
import static java.lang.Math.floorMod;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;
import static java.lang.String.join;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.DAYS;

public class OracleClient
        extends BaseJdbcClient
{
    public static final int ORACLE_MAX_LIST_EXPRESSIONS = 1000;

    private static final int MAX_ORACLE_TIMESTAMP_PRECISION = 9;
    private static final int MAX_BYTES_PER_CHAR = 4;

    private static final int ORACLE_VARCHAR2_MAX_BYTES = 4000;
    private static final int ORACLE_VARCHAR2_MAX_CHARS = ORACLE_VARCHAR2_MAX_BYTES / MAX_BYTES_PER_CHAR;

    private static final int ORACLE_CHAR_MAX_BYTES = 2000;
    private static final int ORACLE_CHAR_MAX_CHARS = ORACLE_CHAR_MAX_BYTES / MAX_BYTES_PER_CHAR;

    private static final int PRECISION_OF_UNSPECIFIED_NUMBER = 127;

    private static final int TRINO_BIGINT_TYPE = 832_424_001;

    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("uuuu-MM-dd");
    private static final DateTimeFormatter TIMESTAMP_SECONDS_FORMATTER = DateTimeFormatter.ofPattern("uuuu-MM-dd HH:mm:ss");

    private static final DateTimeFormatter TIMESTAMP_NANO_OPTIONAL_FORMATTER = new DateTimeFormatterBuilder()
            .appendPattern("uuuu-MM-dd HH:mm:ss")
            .optionalStart()
            .appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true)
            .optionalEnd()
            .toFormatter();

    private static final Set<String> INTERNAL_SCHEMAS = ImmutableSet.<String>builder()
            .add("ctxsys")
            .add("flows_files")
            .add("mdsys")
            .add("outln")
            .add("sys")
            .add("system")
            .add("xdb")
            .add("xs$null")
            .build();

    /**
     * Note the type mappings from trino -> oracle types can cause surprises since they are not invertible
     * For example, creating an oracle table in trino with a bigint column will generate an oracle table with a number column
     * Then querying the oracle table with the number column will return a decimal (not a bigint)
     */
    private static final Map<Type, WriteMapping> WRITE_MAPPINGS = ImmutableMap.<Type, WriteMapping>builder()
            .put(BOOLEAN, oracleBooleanWriteMapping())
            .put(BIGINT, WriteMapping.longMapping("number(19)", bigintWriteFunction()))
            .put(INTEGER, WriteMapping.longMapping("number(10)", integerWriteFunction()))
            .put(SMALLINT, WriteMapping.longMapping("number(5)", smallintWriteFunction()))
            .put(TINYINT, WriteMapping.longMapping("number(3)", tinyintWriteFunction()))
            .put(DOUBLE, WriteMapping.doubleMapping("binary_double", oracleDoubleWriteFunction()))
            .put(REAL, WriteMapping.longMapping("binary_float", oracleRealWriteFunction()))
            .put(VARBINARY, WriteMapping.sliceMapping("blob", varbinaryWriteFunction()))
            .put(DATE, WriteMapping.longMapping("date", trinoDateToOracleDateWriteFunction()))
            .put(TIMESTAMP_TZ_MILLIS, WriteMapping.longMapping("timestamp(3) with time zone", oracleTimestampWithTimeZoneWriteFunction()))
            .buildOrThrow();

    private final boolean synonymsEnabled;
    private final ConnectorExpressionRewriter<ParameterizedExpression> connectorExpressionRewriter;
    private final AggregateFunctionRewriter<JdbcExpression, ?> aggregateFunctionRewriter;
    private final Optional<Integer> fetchSize;

    @Inject
    public OracleClient(
            BaseJdbcConfig config,
            OracleConfig oracleConfig,
            ConnectionFactory connectionFactory,
            QueryBuilder queryBuilder,
            IdentifierMapping identifierMapping,
            RemoteQueryModifier queryModifier)
    {
        super("\"", connectionFactory, queryBuilder, config.getJdbcTypesMappedToVarchar(), identifierMapping, queryModifier, true);

        this.synonymsEnabled = oracleConfig.isSynonymsEnabled();

        this.connectorExpressionRewriter = JdbcConnectorExpressionRewriterBuilder.newBuilder()
                .addStandardRules(this::quoted)
                .withTypeClass("numeric_type", ImmutableSet.of("tinyint", "smallint", "integer", "bigint", "decimal", "real", "double"))
                .map("$equal(left: numeric_type, right: numeric_type)").to("left = right")
                .map("$not_equal(left: numeric_type, right: numeric_type)").to("left <> right")
                .map("$less_than(left: numeric_type, right: numeric_type)").to("left < right")
                .map("$less_than_or_equal(left: numeric_type, right: numeric_type)").to("left <= right")
                .map("$greater_than(left: numeric_type, right: numeric_type)").to("left > right")
                .map("$greater_than_or_equal(left: numeric_type, right: numeric_type)").to("left >= right")
                .add(new RewriteStringComparison())
                .build();

        JdbcTypeHandle bigintTypeHandle = new JdbcTypeHandle(TRINO_BIGINT_TYPE, Optional.of("NUMBER"), Optional.of(0), Optional.of(0), Optional.empty(), Optional.empty());
        this.aggregateFunctionRewriter = new AggregateFunctionRewriter<>(
                connectorExpressionRewriter,
                ImmutableSet.<AggregateFunctionRule<JdbcExpression, ParameterizedExpression>>builder()
                        .add(new ImplementCountAll(bigintTypeHandle))
                        .add(new ImplementCount(bigintTypeHandle))
                        .add(new ImplementCountDistinct(bigintTypeHandle, true))
                        .add(new ImplementMinMax(true))
                        .add(new ImplementSum(OracleClient::toTypeHandle))
                        .add(new ImplementAvgFloatingPoint())
                        .add(new ImplementAvgDecimal())
                        .add(new ImplementStddevSamp())
                        .add(new ImplementStddevPop())
                        .add(new ImplementVarianceSamp())
                        .add(new ImplementVariancePop())
                        .add(new ImplementCovarianceSamp())
                        .add(new ImplementCovariancePop())
                        .build());

        this.fetchSize = oracleConfig.getFetchSize();
    }

    @Override
    protected Optional<List<String>> getTableTypes()
    {
        if (synonymsEnabled) {
            return Optional.of(ImmutableList.of("TABLE", "VIEW", "SYNONYM"));
        }
        return Optional.of(ImmutableList.of("TABLE", "VIEW"));
    }

    @Override
    public Map<String, Object> getTableProperties(ConnectorSession session, JdbcTableHandle tableHandle)
    {
        return super.getTableProperties(session, tableHandle);
    }

    @Override
    protected boolean filterSchema(String schemaName)
    {
        if (INTERNAL_SCHEMAS.contains(schemaName.toLowerCase(ENGLISH))) {
            return false;
        }
        return super.filterSchema(schemaName);
    }

    @Override
    public PreparedStatement getPreparedStatement(Connection connection, String sql, Optional<Integer> columnCount)
            throws SQLException
    {
        PreparedStatement statement = connection.prepareStatement(sql);
        // This is a heuristic, not exact science. A better formula can perhaps be found with measurements.
        // Column count is not known for non-SELECT queries. Not setting fetch size for these.
        Optional<Integer> fetchSize = Optional.ofNullable(this.fetchSize.orElseGet(() ->
                columnCount.map(count -> max(100_000 / count, 1_000))
                        .orElse(null)));
        if (fetchSize.isPresent()) {
            statement.setFetchSize(fetchSize.get());
        }
        return statement;
    }

    @Override
    protected void renameTable(ConnectorSession session, Connection connection, String catalogName, String remoteSchemaName, String remoteTableName, String newRemoteSchemaName, String newRemoteTableName)
            throws SQLException
    {
        if (!remoteSchemaName.equals(newRemoteSchemaName)) {
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support renaming tables across schemas");
        }

        String newTableName = newRemoteTableName.toUpperCase(ENGLISH);
        execute(session, connection, format(
                "ALTER TABLE %s RENAME TO %s",
                quoted(catalogName, remoteSchemaName, remoteTableName),
                quoted(newTableName)));
    }

    @Override
    public void createSchema(ConnectorSession session, String schemaName)
    {
        // ORA-02420: missing schema authorization clause
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support creating schemas");
    }

    @Override
    public void dropSchema(ConnectorSession session, String schemaName, boolean cascade)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support dropping schemas");
    }

    @Override
    protected void dropTable(ConnectorSession session, RemoteTableName remoteTableName, boolean temporaryTable)
    {
        String quotedTable = quoted(remoteTableName);
        String dropTableSql = "DROP TABLE " + quotedTable;
        try (Connection connection = connectionFactory.openConnection(session)) {
            if (temporaryTable) {
                // Turn off auto-commit so the lock is held until after the DROP
                connection.setAutoCommit(false);
                // By default, when dropping a table, oracle does not wait for the table lock.
                // If another transaction is using the table at the same time, DROP TABLE will throw.
                // The solution is to first lock the table, waiting for other active transactions to complete.
                // In Oracle, DDL automatically commits, so DROP TABLE will release the lock afterwards.
                // NOTE: We can only lock tables owned by trino, hence only doing this for temporary tables.
                execute(session, connection, "LOCK TABLE " + quotedTable + " IN EXCLUSIVE MODE");
                // Oracle puts dropped tables into a recycling bin, which keeps them accessible for a period of time.
                // PURGE will bypass the bin and completely delete the table immediately.
                // We should only PURGE the table if it is a temporary table that trino created,
                // as purging all dropped tables may be unexpected behavior for our clients.
                dropTableSql += " PURGE";
            }
            execute(session, connection, dropTableSql);
            // Commit the transaction (for temporaryTables), or a no-op for regular tables.
            // This is better than connection.commit() because you're not supposed to commit() if autoCommit is true.
            connection.setAutoCommit(true);
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
    }

    @Override
    public void renameSchema(ConnectorSession session, String schemaName, String newSchemaName)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support renaming schemas");
    }

    @Override
    public OptionalInt getMaxColumnNameLength(ConnectorSession session)
    {
        return getMaxColumnNameLengthFromDatabaseMetaData(session);
    }

    @Override
    public Optional<String> getTableComment(ResultSet resultSet)
            throws SQLException
    {
        return Optional.ofNullable(emptyToNull(resultSet.getString("REMARKS")));
    }

    @Override
    protected List<String> createTableSqls(RemoteTableName remoteTableName, List<String> columns, ConnectorTableMetadata tableMetadata)
    {
        //checkArgument(tableMetadata.getProperties().isEmpty(), "Unsupported table properties: %s", tableMetadata.getProperties());
        java.util.Set<String> propKeys0 = tableMetadata.getProperties().keySet();
        HashSet<String> propKeys = new HashSet<String>(propKeys0);
        if (propKeys.contains("index")) {
            propKeys.remove("index");
        }
        checkArgument(propKeys.isEmpty(), "Unsupported table properties: %s", propKeys.toString());
        ImmutableList.Builder<String> createTableSqlsBuilder = ImmutableList.builder();
        createTableSqlsBuilder.add(format("CREATE TABLE %s (%s)", quoted(remoteTableName), join(", ", columns)));
        Optional<String> tableComment = tableMetadata.getComment();
        if (tableMetadata.getProperties().containsKey("index")) {
            List<String> indexspecs = (List<String>) tableMetadata.getProperties().get("index");
            for (String indexSpec : indexspecs) {
                int pos1 = indexSpec.indexOf("(");
                int pos2 = indexSpec.indexOf(")");
                if ((pos1 < 0) || (pos2 < 0)) {
                    throw new TrinoException(JDBC_ERROR, "" +
                            "Index spec invalid format, expected indexname(col1,col2,...) but found "
                            + indexSpec);
                }
                String indexColumns = indexSpec.substring(pos1 + 1, pos2);
                String tableName = remoteTableName.getTableName();
                if (tableName.length() >= 5) {
                    tableName = tableName.substring(tableName.length() - 5);
                }

                String indexName = "I" + tableName + "_" + indexSpec.substring(0, pos1);
                createTableSqlsBuilder.add(
                        format("CREATE INDEX %s ON %s(%s)",
                                quoted(indexName),
                                quoted(remoteTableName),
                                indexColumns));
            }
        }
        if (tableComment.isPresent()) {
            createTableSqlsBuilder.add(buildTableCommentSql(remoteTableName, tableComment));
        }
        return createTableSqlsBuilder.build();
    }

    @Override
    public void setTableComment(ConnectorSession session, JdbcTableHandle handle, Optional<String> comment)
    {
        execute(session, buildTableCommentSql(handle.asPlainTable().getRemoteTableName(), comment));
    }

    private String buildTableCommentSql(RemoteTableName remoteTableName, Optional<String> comment)
    {
        return format(
                "COMMENT ON TABLE %s IS %s",
                quoted(remoteTableName),
                varcharLiteral(comment.orElse("")));
    }

    @Override
    public Optional<ColumnMapping> toColumnMapping(ConnectorSession session, Connection connection, JdbcTypeHandle typeHandle)
    {
        if (typeHandle.jdbcType() == TRINO_BIGINT_TYPE) {
            // Synthetic column
            return Optional.of(bigintColumnMapping());
        }

        String jdbcTypeName = typeHandle.jdbcTypeName()
                .orElseThrow(() -> new TrinoException(JDBC_ERROR, "Type name is missing: " + typeHandle));

        Optional<ColumnMapping> mappingToVarchar = getForcedMappingToVarchar(typeHandle);
        if (mappingToVarchar.isPresent()) {
            return mappingToVarchar;
        }

        if (jdbcTypeName.equalsIgnoreCase("date")) {
            return Optional.of(ColumnMapping.longMapping(
                    TIMESTAMP_SECONDS,
                    oracleTimestampReadFunction(TIMESTAMP_SECONDS),
                    trinoTimestampToOracleDateWriteFunction(),
                    FULL_PUSHDOWN));
        }

        switch (typeHandle.jdbcType()) {
            case Types.SMALLINT:
                return Optional.of(ColumnMapping.longMapping(
                        SMALLINT,
                        ResultSet::getShort,
                        smallintWriteFunction(),
                        FULL_PUSHDOWN));
            case OracleTypes.BINARY_FLOAT:
                return Optional.of(ColumnMapping.longMapping(
                        REAL,
                        (resultSet, columnIndex) -> floatToRawIntBits(resultSet.getFloat(columnIndex)),
                        oracleRealWriteFunction(),
                        FULL_PUSHDOWN));

            case OracleTypes.BINARY_DOUBLE:
            case OracleTypes.FLOAT:
                return Optional.of(ColumnMapping.doubleMapping(
                        DOUBLE,
                        ResultSet::getDouble,
                        oracleDoubleWriteFunction(),
                        FULL_PUSHDOWN));
            case OracleTypes.NUMBER:
                int actualPrecision = typeHandle.requiredColumnSize();
                int decimalDigits = typeHandle.requiredDecimalDigits();
                // Map negative scale to decimal(p+s, 0).
                int precision = actualPrecision + max(-decimalDigits, 0);
                int scale = max(decimalDigits, 0);
                Optional<Integer> numberDefaultScale = getNumberDefaultScale(session);
                RoundingMode roundingMode = getNumberRoundingMode(session);
                if (precision < scale) {
                    if (roundingMode == RoundingMode.UNNECESSARY) {
                        break;
                    }
                    scale = min(Decimals.MAX_PRECISION, scale);
                    precision = scale;
                }
                else if (numberDefaultScale.isPresent() && precision == PRECISION_OF_UNSPECIFIED_NUMBER) {
                    precision = Decimals.MAX_PRECISION;
                    scale = numberDefaultScale.get();
                }
                else if (precision > Decimals.MAX_PRECISION || actualPrecision <= 0) {
                    break;
                }
                DecimalType decimalType = createDecimalType(precision, scale);
                // JDBC driver can return BigDecimal with lower scale than column's scale when there are trailing zeroes
                if (decimalType.isShort()) {
                    return Optional.of(ColumnMapping.longMapping(
                            decimalType,
                            shortDecimalReadFunction(decimalType, roundingMode),
                            shortDecimalWriteFunction(decimalType),
                            FULL_PUSHDOWN));
                }
                return Optional.of(ColumnMapping.objectMapping(
                        decimalType,
                        longDecimalReadFunction(decimalType, roundingMode),
                        longDecimalWriteFunction(decimalType),
                        FULL_PUSHDOWN));

            case OracleTypes.CHAR:
            case OracleTypes.NCHAR:
                CharType charType = createCharType(typeHandle.requiredColumnSize());
                return Optional.of(ColumnMapping.sliceMapping(
                        charType,
                        charReadFunction(charType),
                        oracleCharWriteFunction(),
                        FULL_PUSHDOWN));

            case OracleTypes.VARCHAR:
            case OracleTypes.NVARCHAR:
                return Optional.of(ColumnMapping.sliceMapping(
                        createVarcharType(typeHandle.requiredColumnSize()),
                        (varcharResultSet, varcharColumnIndex) -> utf8Slice(varcharResultSet.getString(varcharColumnIndex)),
                        varcharWriteFunction(),
                        FULL_PUSHDOWN));

            case OracleTypes.CLOB:
            case OracleTypes.NCLOB:
                return Optional.of(ColumnMapping.sliceMapping(
                        createUnboundedVarcharType(),
                        (resultSet, columnIndex) -> utf8Slice(resultSet.getString(columnIndex)),
                        varcharWriteFunction(),
                        DISABLE_PUSHDOWN));

            case OracleTypes.VARBINARY: // Oracle's RAW(n)
            case OracleTypes.BLOB:
                return Optional.of(ColumnMapping.sliceMapping(
                        VARBINARY,
                        (resultSet, columnIndex) -> wrappedBuffer(resultSet.getBytes(columnIndex)),
                        varbinaryWriteFunction(),
                        DISABLE_PUSHDOWN));

            case OracleTypes.TIMESTAMP:
                int timestampPrecision = typeHandle.requiredDecimalDigits();
                return Optional.of(oracleTimestampColumnMapping(createTimestampType(timestampPrecision)));
            case OracleTypes.TIMESTAMPTZ:
                return Optional.of(oracleTimestampWithTimeZoneColumnMapping());
        }
        if (getUnsupportedTypeHandling(session) == CONVERT_TO_VARCHAR) {
            return mapToUnboundedVarchar(typeHandle);
        }
        return Optional.empty();
    }

    private static ColumnMapping oracleTimestampColumnMapping(TimestampType timestampType)
    {
        if (timestampType.isShort()) {
            return ColumnMapping.longMapping(
                    timestampType,
                    oracleTimestampReadFunction(timestampType),
                    oracleTimestampWriteFunction(timestampType),
                    FULL_PUSHDOWN);
        }
        return ColumnMapping.objectMapping(
                timestampType,
                oracleLongTimestampReadFunction(timestampType),
                oracleLongTimestampWriteFunction(timestampType),
                FULL_PUSHDOWN);
    }

    @Override
    public Optional<JdbcExpression> implementAggregation(ConnectorSession session, AggregateFunction aggregate, Map<String, ColumnHandle> assignments)
    {
        return aggregateFunctionRewriter.rewrite(session, aggregate, assignments);
    }

    @Override
    public Optional<ParameterizedExpression> convertPredicate(ConnectorSession session, ConnectorExpression expression, Map<String, ColumnHandle> assignments)
    {
        return connectorExpressionRewriter.rewrite(session, expression, assignments);
    }

    private static Optional<JdbcTypeHandle> toTypeHandle(DecimalType decimalType)
    {
        return Optional.of(new JdbcTypeHandle(OracleTypes.NUMBER, Optional.of("NUMBER"), Optional.of(decimalType.getPrecision()), Optional.of(decimalType.getScale()), Optional.empty(), Optional.empty()));
    }

    @Override
    protected Optional<BiFunction<String, Long, String>> limitFunction()
    {
        return Optional.of((sql, limit) -> format("SELECT * FROM (%s) WHERE ROWNUM <= %s", sql, limit));
    }

    @Override
    public boolean isLimitGuaranteed(ConnectorSession session)
    {
        return true;
    }

    @Override
    protected boolean isSupportedJoinCondition(ConnectorSession session, JdbcJoinCondition joinCondition)
    {
        return joinCondition.getOperator() != JoinCondition.Operator.IDENTICAL;
    }

    public static LongWriteFunction trinoDateToOracleDateWriteFunction()
    {
        return new LongWriteFunction()
        {
            @Override
            public String getBindExpression()
            {
                return "TO_DATE(?, 'SYYYY-MM-DD')";
            }

            @Override
            public void set(PreparedStatement statement, int index, long value)
                    throws SQLException
            {
                long utcMillis = DAYS.toMillis(value);
                LocalDateTime date = LocalDateTime.from(Instant.ofEpochMilli(utcMillis).atZone(ZoneOffset.UTC));
                statement.setString(index, DATE_FORMATTER.format(date));
            }

            @Override
            public void setNull(PreparedStatement statement, int index)
                    throws SQLException
            {
                statement.setNull(index, Types.VARCHAR);
            }
        };
    }

    private static LongWriteFunction trinoTimestampToOracleDateWriteFunction()
    {
        return new LongWriteFunction()
        {
            @Override
            public String getBindExpression()
            {
                // Oracle's DATE stores year, month, day, hour, minute, seconds, but not second fraction
                return "TO_DATE(?, 'SYYYY-MM-DD HH24:MI:SS')";
            }

            @Override
            public void set(PreparedStatement statement, int index, long value)
                    throws SQLException
            {
                long epochSecond = floorDiv(value, MICROSECONDS_PER_SECOND);
                int microsOfSecond = floorMod(value, MICROSECONDS_PER_SECOND);
                verify(microsOfSecond == 0, "Micros of second must be zero: '%s'", value);
                LocalDateTime localDateTime = LocalDateTime.ofEpochSecond(epochSecond, 0, ZoneOffset.UTC);
                statement.setString(index, TIMESTAMP_SECONDS_FORMATTER.format(localDateTime));
            }

            @Override
            public void setNull(PreparedStatement statement, int index)
                    throws SQLException
            {
                statement.setNull(index, Types.VARCHAR);
            }
        };
    }

    private static ObjectWriteFunction oracleLongTimestampWriteFunction(TimestampType timestampType)
    {
        int precision = timestampType.getPrecision();
        verifyLongTimestampPrecision(timestampType);

        return new ObjectWriteFunction() {
            @Override
            public Class<?> getJavaType()
            {
                return LongTimestamp.class;
            }

            @Override
            public void set(PreparedStatement statement, int index, Object value)
                    throws SQLException
            {
                LocalDateTime timestamp = fromLongTrinoTimestamp((LongTimestamp) value, precision);
                statement.setString(index, TIMESTAMP_NANO_OPTIONAL_FORMATTER.format(timestamp));
            }

            @Override
            public String getBindExpression()
            {
                return getOracleBindExpression(precision);
            }

            @Override
            public void setNull(PreparedStatement statement, int index)
                    throws SQLException
            {
                statement.setNull(index, Types.VARCHAR);
            }
        };
    }

    private static LongWriteFunction oracleTimestampWriteFunction(TimestampType timestampType)
    {
        return new LongWriteFunction()
        {
            @Override
            public String getBindExpression()
            {
                return getOracleBindExpression(timestampType.getPrecision());
            }

            @Override
            public void set(PreparedStatement statement, int index, long epochMicros)
                    throws SQLException
            {
                LocalDateTime timestamp = fromTrinoTimestamp(epochMicros);
                statement.setString(index, TIMESTAMP_NANO_OPTIONAL_FORMATTER.format(timestamp));
            }

            @Override
            public void setNull(PreparedStatement statement, int index)
                    throws SQLException
            {
                statement.setNull(index, Types.VARCHAR);
            }
        };
    }

    private static String getOracleBindExpression(int precision)
    {
        if (precision == 0) {
            return "TO_TIMESTAMP(?, 'SYYYY-MM-DD HH24:MI:SS')";
        }
        if (precision <= 2) {
            return "TO_TIMESTAMP(?, 'SYYYY-MM-DD HH24:MI:SS.FF')";
        }

        return format("TO_TIMESTAMP(?, 'SYYYY-MM-DD HH24:MI:SS.FF%d')", precision);
    }

    private static LongReadFunction oracleTimestampReadFunction(TimestampType timestampType)
    {
        return (resultSet, columnIndex) -> {
            LocalDateTime timestamp = resultSet.getObject(columnIndex, LocalDateTime.class);
            // Adjust years when the value is B.C. dates because Oracle returns +1 year unless converting to string in their server side
            if (timestamp.getYear() <= 0) {
                timestamp = timestamp.minusYears(1);
            }
            return toTrinoTimestamp(timestampType, timestamp);
        };
    }

    private static ObjectReadFunction oracleLongTimestampReadFunction(TimestampType timestampType)
    {
        verifyLongTimestampPrecision(timestampType);
        return ObjectReadFunction.of(
                LongTimestamp.class,
                (resultSet, columnIndex) -> {
                    LocalDateTime timestamp = resultSet.getObject(columnIndex, LocalDateTime.class);
                    // Adjust years when the value is B.C. dates because Oracle returns +1 year unless converting to string in their server side
                    if (timestamp.getYear() <= 0) {
                        timestamp = timestamp.minusYears(1);
                    }
                    return toLongTrinoTimestamp(timestampType, timestamp);
                });
    }

    private static void verifyLongTimestampPrecision(TimestampType timestampType)
    {
        int precision = timestampType.getPrecision();
        checkArgument(precision > MAX_SHORT_PRECISION && precision <= MAX_ORACLE_TIMESTAMP_PRECISION,
                "Precision is out of range: %s", precision);
    }

    public static ColumnMapping oracleTimestampWithTimeZoneColumnMapping()
    {
        return ColumnMapping.longMapping(
                TIMESTAMP_TZ_MILLIS,
                (resultSet, columnIndex) -> {
                    ZonedDateTime timestamp = resultSet.getObject(columnIndex, ZonedDateTime.class);
                    return packDateTimeWithZone(
                            timestamp.toInstant().toEpochMilli(),
                            timestamp.getZone().getId());
                },
                oracleTimestampWithTimeZoneWriteFunction(),
                FULL_PUSHDOWN);
    }

    public static LongWriteFunction oracleTimestampWithTimeZoneWriteFunction()
    {
        return LongWriteFunction.of(OracleTypes.TIMESTAMPTZ, (statement, index, encodedTimeWithZone) -> {
            Instant time = Instant.ofEpochMilli(unpackMillisUtc(encodedTimeWithZone));
            ZoneId zone = unpackZoneKey(encodedTimeWithZone).getZoneId();
            statement.setObject(index, time.atZone(zone));
        });
    }

    private static WriteMapping oracleBooleanWriteMapping()
    {
        return WriteMapping.booleanMapping("number(1)", oracleBooleanWriteFunction());
    }

    private static BooleanWriteFunction oracleBooleanWriteFunction()
    {
        return BooleanWriteFunction.of(Types.TINYINT, (statement, index, value) -> statement.setInt(index, value ? 1 : 0));
    }

    public static LongWriteFunction oracleRealWriteFunction()
    {
        return LongWriteFunction.of(Types.REAL, (statement, index, value) ->
                statement.unwrap(OraclePreparedStatement.class).setBinaryFloat(index, intBitsToFloat(toIntExact(value))));
    }

    public static DoubleWriteFunction oracleDoubleWriteFunction()
    {
        return DoubleWriteFunction.of(Types.DOUBLE, (statement, index, value) ->
                statement.unwrap(OraclePreparedStatement.class).setBinaryDouble(index, value));
    }

    private SliceWriteFunction oracleCharWriteFunction()
    {
        return SliceWriteFunction.of(Types.NCHAR, (statement, index, value) -> statement.unwrap(OraclePreparedStatement.class).setFixedCHAR(index, value.toStringUtf8()));
    }

    @Override
    public WriteMapping toWriteMapping(ConnectorSession session, Type type)
    {
        if (type instanceof VarcharType varcharType) {
            String dataType;
            if (varcharType.isUnbounded() || varcharType.getBoundedLength() > ORACLE_VARCHAR2_MAX_CHARS) {
                dataType = "nclob";
            }
            else {
                dataType = "varchar2(" + varcharType.getBoundedLength() + " CHAR)";
            }
            return WriteMapping.sliceMapping(dataType, varcharWriteFunction());
        }
        if (type instanceof CharType charType) {
            String dataType;
            if (charType.getLength() > ORACLE_CHAR_MAX_CHARS) {
                dataType = "nclob";
            }
            else {
                dataType = "char(" + charType.getLength() + " CHAR)";
            }
            return WriteMapping.sliceMapping(dataType, oracleCharWriteFunction());
        }
        if (type instanceof DecimalType decimalType) {
            String dataType = format("number(%s, %s)", decimalType.getPrecision(), decimalType.getScale());
            if (decimalType.isShort()) {
                return WriteMapping.longMapping(dataType, shortDecimalWriteFunction(decimalType));
            }
            return WriteMapping.objectMapping(dataType, longDecimalWriteFunction(decimalType));
        }
        if (type instanceof TimestampType timestampType) {
            if (type.equals(TIMESTAMP_SECONDS)) {
                // Specify 'date' instead of 'timestamp(0)' to propagate the type in case of CTAS from date columns
                // Oracle date stores year, month, day, hour, minute, seconds, but not second fraction
                return WriteMapping.longMapping("date", trinoTimestampToOracleDateWriteFunction());
            }
            int precision = min(timestampType.getPrecision(), MAX_ORACLE_TIMESTAMP_PRECISION);
            String dataType = format("timestamp(%d)", precision);
            if (timestampType.isShort()) {
                return WriteMapping.longMapping(dataType, oracleTimestampWriteFunction(timestampType));
            }
            return WriteMapping.objectMapping(dataType, oracleLongTimestampWriteFunction(createTimestampType(precision)));
        }
        WriteMapping writeMapping = WRITE_MAPPINGS.get(type);
        if (writeMapping != null) {
            return writeMapping;
        }
        throw new TrinoException(NOT_SUPPORTED, "Unsupported column type: " + type.getDisplayName());
    }

    @Override
    public void setColumnComment(ConnectorSession session, JdbcTableHandle handle, JdbcColumnHandle column, Optional<String> comment)
    {
        // Oracle doesn't support prepared statement for COMMENT statement
        String sql = format(
                "COMMENT ON COLUMN %s.%s IS %s",
                quoted(handle.asPlainTable().getRemoteTableName()),
                quoted(column.getColumnName()),
                varcharLiteral(comment.orElse("")));
        execute(session, sql);
    }

    @Override
    public void setColumnType(ConnectorSession session, JdbcTableHandle handle, JdbcColumnHandle column, Type type)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support setting column types");
    }

    @Override
    public void dropNotNullConstraint(ConnectorSession session, JdbcTableHandle handle, JdbcColumnHandle column)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support dropping a not null constraint");
    }

    public void gatherTableStats(ConnectorSession session, String schemaName, String tableName)
    {
        String sql = "BEGIN DBMS_STATS.GATHER_TABLE_STATS('" + schemaName + "', '" + tableName + "'); END;";
        try (Connection connection = connectionFactory.openConnection(session)) {
            try (Statement statement = connection.createStatement()) {
                String modifiedQuery = queryModifier.apply(session, sql);
                //log.debug("Execute: %s", modifiedQuery);
                statement.execute(modifiedQuery);
            }
            catch (SQLException e) {
                e.addSuppressed(new RuntimeException("Query: " + sql));
                throw e;
            }
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
    }

    @Override
    public ConnectorSplitSource getSplits(ConnectorSession session, JdbcTableHandle tableHandle)
    {
        if (!OracleSessionProperties.getExperimentalSplit(session)) {
            return super.getSplits(session, tableHandle);
        }
        Optional<List<RangeInfo>> splitRanges = getSplitRanges(session, tableHandle);
        if (!splitRanges.isPresent()) {
            return super.getSplits(session, tableHandle);
        }
        List<RangeInfo> splitRange1 = splitRanges.get();
        if (splitRange1.isEmpty()) {
            return super.getSplits(session, tableHandle);
        }
        List<JdbcSplit> splits = splitRange1.stream()
                .map(OracleClient::convertRangeInfoIntoPredicate)
                .map(x -> new JdbcSplit(Optional.of(x)))
                .collect(Collectors.toList());
        return new FixedSplitSource(splits);
    }

    static String convertRangeInfoIntoPredicate(RangeInfo rangeInfo)
    {
        StringBuilder sql = new StringBuilder();
        String expression = rangeInfo.getExpression();
        if (rangeInfo.getLowerBound().isPresent()) {
            if (rangeInfo.getLowerBound().get() instanceof Number) {
                sql.append(expression).append(" >= ").append(rangeInfo.getLowerBound().get());
            }
            else {
                sql.append(expression).append(" >= '").append(rangeInfo.getLowerBound().get()).append("'");
            }
        }

        if (rangeInfo.getUpperBound().isPresent()) {
            if (rangeInfo.getLowerBound().isPresent()) {
                sql.append(" AND ");
            }
            var upperOperator = " < ";
            if (rangeInfo.isUpperInclusive()) {
                upperOperator = " <= ";
            }
            if (rangeInfo.getUpperBound().get() instanceof Number) {
                sql.append(expression).append(upperOperator).append(rangeInfo.getUpperBound().get());
            }
            else {
                sql.append(expression).append(upperOperator).append("'").append(rangeInfo.getUpperBound().get()).append("'");
            }
        }

        return sql.toString();
    }

    Optional<List<RangeInfo>> getSplitRangesFromPrepQuery(Connection connection, PreparedQuery query, JdbcTableHandle tableHandle, SplittingRule rule)
    {
        Logger log = Logger.get(OracleClient.class);
        if (!queryToInfo.containsKey(query)) {
            log.info("QueryEnhancedInfo not found in cache : " + query.toString());
            return Optional.empty();
        }
        var queryInfo = queryToInfo.get(query);
        return getSplitRangesFromQueryInfo(connection, queryInfo, rule);
    }

    private Optional<List<RangeInfo>> getSplitRangesFromQueryInfo(Connection connection, QueryEnhancedInfo queryInfo, SplittingRule rule)
    {
        Logger log = Logger.get(OracleClient.class);
        if (queryInfo == null) {
            return Optional.empty();
        }
        if (queryInfo.isNamedRelation) {
            return getRangeInfos(queryInfo.namedRelation, connection, rule);
        }
        else {
            if (queryInfo.isJoin) {
                Optional<List<RangeInfo>> rightRange = getSplitRangesFromQueryInfo(connection, queryInfo.rightInfo, rule);
                // heuristic : choose non-blank range
                //if (leftRange.isEmpty()) {
                //    return adjustRange(rightRange, queryInfo.rightProjections);
                //}
                if (rightRange.isEmpty()) {
                    Optional<List<RangeInfo>> leftRange = getSplitRangesFromQueryInfo(connection, queryInfo.leftInfo, rule);
                    return adjustRange(leftRange, queryInfo.leftProjections);
                }
                return adjustRange(rightRange, queryInfo.rightProjections);
                // heuristic : choose build-side (right-side) partitioning
            }
            else {
                log.info("unknown query info, should be <<FATAL>> ");
                return Optional.empty();
            }
        }
    }

    private Optional<List<RangeInfo>> adjustRange(Optional<List<RangeInfo>> rangeInfoList, Map<JdbcColumnHandle, String> projections)
    {
        if (rangeInfoList.isEmpty() || (rangeInfoList.get().size() == 0)) {
            return rangeInfoList; // if empty, do nothing
        }
        var rangeInfoList1 = rangeInfoList.get();
        var firstRange = rangeInfoList1.get(0);
        var columnPart = firstRange.expression;
        for (var key : projections.keySet()) {
            if (key.getColumnName().equalsIgnoreCase(columnPart)) {
                // bingo
                columnPart = projections.get(key);
                break;
            }
        }

        for (int i = 0; i < rangeInfoList1.size(); i++) {
            var range = rangeInfoList1.get(i);
            rangeInfoList1.set(i, new RangeInfo(columnPart, range.getLowerBound(), range.getUpperBound()));
            // replace elements
        }
        return rangeInfoList; // which contract did I break?
    }

    public Optional<List<RangeInfo>> getSplitRanges(ConnectorSession session, JdbcTableHandle tableHandle)
    {
        Logger log0 = Logger.get(OracleClient.class);
        Optional<List<RangeInfo>> result0 = Optional.empty();
        log0.info("getSplitRanges: for " + tableHandle.toString());
        Connection connection = null;
        String splitRule = OracleSessionProperties.getSplitRule(session);
        var rules = parseRules(splitRule);
        connection = getConnectionForTableHandle(session, tableHandle, connection, log0);
        if (connection == null) {
            log0.info(" unable to get connection in getSplitRanges ");
            return Optional.empty();
        }

        if (!(tableHandle.getRelationHandle() instanceof JdbcNamedRelationHandle)) {
            JdbcRelationHandle relationHandle = tableHandle.getRelationHandle();
            if (relationHandle instanceof JdbcQueryRelationHandle) {
                var queryHandle = (JdbcQueryRelationHandle) relationHandle;
                var preparedQuery = queryHandle.getPreparedQuery();
                result0 = getSplitRangesFromPrepQuery(connection, preparedQuery, tableHandle, rules);
            }
            else {
                log0.info("unknown relationHandle  => " + relationHandle.toString());
            }
            return result0;
        }

        JdbcNamedRelationHandle tableRelationHandle = tableHandle.getRequiredNamedRelation();

        var rangeInfos = getRangeInfos(tableRelationHandle, connection, rules);
        try {
            connection.close();
        }
        catch (Exception ex) {
            throw new RuntimeException("getSplitRange failed", ex);
        }
        return rangeInfos;
    }

    private Optional<List<RangeInfo>> getRangeInfos(JdbcNamedRelationHandle tableRelationHandle, Connection connection, SplittingRule rules)
    {
        var thisTableName = tableRelationHandle.getSchemaTableName().getTableName();
        for (SplittingRule.Rule r : rules.rules) {
            boolean tableNameMatch = r.isAnyTable() ||
                    r.tableName
                    .equalsIgnoreCase(thisTableName);
            if (!tableNameMatch) {
                continue;
            }
            switch (r.ruleType) {
                case SplittingRule.RuleType.ROWID:
                    return getRangeInfosRowid(tableRelationHandle, connection, r.partitions);
                case SplittingRule.RuleType.INDEX:
                    return getRangeInfosIntegerIndex(tableRelationHandle, connection, r.stride, r.colOrIdx);
                case SplittingRule.RuleType.NTILE:
                    return getRangeInfosNtileIndex(tableRelationHandle, connection, r.partitions, r.colOrIdx);
                default:
                    return Optional.empty();
            }
        }
        return Optional.empty();
    }

    private static SplittingRule parseRules(String splitRule)
    {
        var rules = new SplittingRule();
        try {
            var splitRules = splitRule.split(";");
            for (int i = 0; i < splitRules.length; i++) {
                StringTokenizer tk = new StringTokenizer(splitRules[i], ":(), ");
                String ruleName = tk.hasMoreTokens() ? tk.nextToken() : ""; // INDEX:
                String tableName = tk.hasMoreTokens() ? tk.nextToken() : "*"; // TABLENAME(

                if (ruleName.equalsIgnoreCase("INDEX")) {
                    String colOrIdx = tk.hasMoreTokens() ? tk.nextToken() : "ROWNO"; // rowno
                    int stride = 50000;
                    if (tk.hasMoreTokens()) {
                        stride = Integer.parseInt(tk.nextToken()); // ,1000
                    }
                    rules.newRule(tableName, SplittingRule.RuleType.INDEX)
                            .withColOrIdx(colOrIdx)
                            .withStride(stride);
                    //rangeInfos = getRangeInfosIntegerIndex(tableRelationHandle, connection, stride, colOrIdx);
                }
                else if (ruleName.equalsIgnoreCase("ROWID")) {
                    int partcnt = !tk.hasMoreTokens()
                            ? 100 :
                            Integer.valueOf(tk.nextToken());
                    rules.newRule(tableName, SplittingRule.RuleType.ROWID).withPartitions(partcnt);
                    //rangeInfos = getRangeInfosRowid(tableRelationHandle, connection, partcnt);
                }
                else if (ruleName.equalsIgnoreCase("NTILE")) { // NTILE:*(ROWNO,100)
                    String colOrIdx = tk.hasMoreTokens() ? tk.nextToken() : "ROWNO"; // rowno
                    int parts = 100;
                    if (tk.hasMoreTokens()) {
                        parts = Integer.parseInt(tk.nextToken()); // ,1000
                    }
                    rules.newRule(tableName, SplittingRule.RuleType.NTILE)
                            .withPartitions(parts)
                            .withColOrIdx(colOrIdx);
                    //rangeInfos = getRangeInfosNtileIndex(tableRelationHandle, connection, parts, colOrIdx);
                }
            }
        }
        catch (Exception x) {
            Logger log0 = Logger.get(OracleClient.class);
            log0.error(" Unable to parse rule : " + splitRule);
        }
        return rules;
    }

    private static Optional<List<RangeInfo>> getRangeInfosIntegerIndex(JdbcNamedRelationHandle tableRelationHandle,
            Connection connection, int stride, String colOrIdx)
    {
        Map<String, List<String>> masterIndex = new HashMap<>();
        Logger log = Logger.get(OracleClient.class);
        log.info("getting split range => " + tableRelationHandle.toString());
        String sql = "select index_name, table_owner, column_name, column_position from all_ind_columns where table_owner=? and table_name=?";
        Optional<List<RangeInfo>> result = Optional.empty();
        try {
            PreparedStatement preparedStatement = connection.prepareStatement(sql);
            var schemaName = tableRelationHandle.getSchemaTableName().getSchemaName().toUpperCase(ENGLISH);
            var tableName = tableRelationHandle.getSchemaTableName().getTableName().toUpperCase(ENGLISH);
            log.info("sql => " + sql + " -- schemaName " + schemaName + " tableName " + tableName);
            preparedStatement.setString(1, schemaName);
            preparedStatement.setString(2, tableName);
            ResultSet rs = preparedStatement.executeQuery();
            while (rs.next()) {
                String indexName = rs.getString(1);
                if (!masterIndex.containsKey(indexName)) {
                    masterIndex.put(indexName, new ArrayList<>());
                }
                masterIndex.get(indexName).add(rs.getString(3));
            }
            String columnPart = null;
            for (Map.Entry<String, List<String>> entry : masterIndex.entrySet()) {
                if (entry.getValue().size() == 1) {
                    String theColumn = entry.getValue().get(0);
                    if (theColumn.contains(colOrIdx) || entry.getKey().contains(colOrIdx)) {
                        columnPart = theColumn;
                    }
                }
            }
            if (columnPart == null) {
                return result;
            }
            rs.close();
            log.info("getSplitRange: split on column " + columnPart);
            preparedStatement.close();
            String sql2 = "SELECT MIN(" + columnPart
                    + ") MINV, MAX(" + columnPart + ") MAXV FROM " + tableRelationHandle.getSchemaTableName().getSchemaName()
                    + "." + tableRelationHandle.getSchemaTableName().getTableName();
            preparedStatement = connection.prepareStatement(sql2);
            ResultSet rs2 = preparedStatement.executeQuery();
            log.info("getSplitRange: executed " + sql2);
            while (rs2.next()) {
                Object ob1 = rs2.getObject(1);
                if (ob1 == null) {
                    return Optional.empty();
                }
                ob1 = rs2.getObject(2);
                if (ob1 == null) {
                    return Optional.empty();
                }
                int minVal = rs2.getInt(1);
                int maxVal = rs2.getInt(2);
                int curPos = minVal;

                log.info("getSplitRange: minVal " + minVal + "maxVal " + maxVal);
                List<RangeInfo> result1 = new ArrayList<>();
                Optional<Integer> lowerBound = Optional.empty();
                Optional<Integer> upperBound = Optional.of(curPos + stride);
                result1.add(new RangeInfo(columnPart, lowerBound, upperBound));
                curPos += stride;
                while (curPos <= maxVal) {
                    lowerBound = upperBound; // = curPos
                    curPos += stride;
                    upperBound = Optional.of(curPos);
                    result1.add(new RangeInfo(columnPart, lowerBound, upperBound));
                }
                for (RangeInfo r : result1) {
                    log.info("getSplitRanges : " + r.toString());
                }
                result = Optional.of(result1);
            }
            rs2.close();
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
        return result;
    }

    private static Optional<List<RangeInfo>> getRangeInfosNtileIndex(JdbcNamedRelationHandle tableRelationHandle,
            Connection connection, int parts, String colOrIdx)
    {
        Map<String, List<String>> masterIndex = new HashMap<>();
        Logger log = Logger.get(OracleClient.class);
        log.info("getting split range => " + tableRelationHandle.toString());
        String sql = "select index_name, table_owner, column_name, column_position from all_ind_columns where table_owner=? and table_name=?";
        Optional<List<RangeInfo>> result = Optional.empty();
        try {
            PreparedStatement preparedStatement = connection.prepareStatement(sql);
            var schemaName = tableRelationHandle.getSchemaTableName().getSchemaName().toUpperCase(ENGLISH);
            var tableName = tableRelationHandle.getSchemaTableName().getTableName().toUpperCase(ENGLISH);
            log.info("sql => " + sql + " -- schemaName " + schemaName + " tableName " + tableName);
            preparedStatement.setString(1, schemaName);
            preparedStatement.setString(2, tableName);
            ResultSet rs = preparedStatement.executeQuery();
            while (rs.next()) {
                String indexName = rs.getString(1);
                if (!masterIndex.containsKey(indexName)) {
                    masterIndex.put(indexName, new ArrayList<>());
                }
                masterIndex.get(indexName).add(rs.getString(3));
            }
            String columnPart = null;
            for (Map.Entry<String, List<String>> entry : masterIndex.entrySet()) {
                if (entry.getValue().size() == 1) {
                    String theColumn = entry.getValue().get(0);
                    if (theColumn.contains(colOrIdx) || entry.getKey().contains(colOrIdx)) {
                        columnPart = theColumn;
                    }
                }
            }
            if (columnPart == null) {
                return Optional.empty();
            }
            rs.close();
            log.info("getSplitRange: split on column " + columnPart);
            preparedStatement.close();
            //select min (vkont) minvkont, max(vkont) maxvkont, bucketno from (select vkont,ntile(100) over (order by vkont) bucketno from sapsr3.fkkvkp) a group by bucketno;
            String sql2 = "SELECT MIN(" + columnPart
                    + ") MINV, MAX(" + columnPart + ") MAXV, BUCKETNO FROM (SELECT "
                    + columnPart + ", NTILE(" + parts + ") over (ORDER BY " + columnPart
                    + ") BUCKETNO FROM " + tableRelationHandle.getSchemaTableName().getSchemaName()
                    + "." + tableRelationHandle.getSchemaTableName().getTableName()
                    + ") A GROUP BY BUCKETNO";
            preparedStatement = connection.prepareStatement(sql2);
            ResultSet rs2 = preparedStatement.executeQuery();
            log.info("getSplitRange: executed " + sql2);
            List<RangeInfo> result1 = new ArrayList<>();
            while (rs2.next()) {
                Object ob1 = rs2.getObject(1);
                if (ob1 == null) {
                    return Optional.empty();
                }
                Object ob2 = rs2.getObject(2);
                if (ob2 == null) {
                    return Optional.empty();
                }
                log.info("getSplitRange: minVal " + ob1 + "maxVal " + ob2);
                result1.add(new RangeInfo(columnPart, Optional.of(ob1), Optional.of(ob2))
                        .withUpperInclusive(true));
            }
            rs2.close();
            result = Optional.of(result1);
            return result;
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
    }

    private static Optional<List<RangeInfo>> getRangeInfosRowid(JdbcNamedRelationHandle tableRelationHandle,
            Connection connection, int fragments)
    {
        Logger log = Logger.get(OracleClient.class);
        log.info("getting split range => " + tableRelationHandle.toString());
        String sql = "select min(rowid1) minrowid, max(rowid1) maxrowid, bucketno from ( select rowid rowid1, ntile("
                + fragments + ") over (order by rowid) bucketno from " + tableRelationHandle.getSchemaTableName().getSchemaName()
                + "." + tableRelationHandle.getSchemaTableName().getTableName() + ") a group by bucketno";
        Optional<List<RangeInfo>> result = Optional.empty();
        try {
            PreparedStatement preparedStatement = connection.prepareStatement(sql);
            log.info("sql => " + sql);
            ResultSet rs = preparedStatement.executeQuery();
            List<RangeInfo> result1 = new ArrayList<>();
            while (rs.next()) {
                String minVal = rs.getString(1);
                String maxVal = rs.getString(2);
                int bucketNo = rs.getInt(3);
                log.info("getSplitRange: bucket " + bucketNo + " minVal " + minVal + "maxVal " + maxVal);
                result1.add(new RangeInfo("ROWID", Optional.of(minVal), Optional.of(maxVal))
                        .withUpperInclusive(true));
            }
            result = Optional.of(result1);
            rs.close();
            log.info("getSplitRange: split on ROWID");

            return result;
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
    }

    private Connection getConnectionForTableHandle(ConnectorSession session, JdbcTableHandle tableHandle, Connection connection, Logger log)
    {
        try {
            connection = getConnection(session, null, tableHandle);
        }
        catch (SQLException se) {
            log.error(se);
            if (connection != null) {
                try {
                    connection.close();
                }
                catch (Exception ex) {
                }
            }
        }
        return connection;
    }

    @Override
    public PreparedQuery prepareQuery(ConnectorSession session, JdbcTableHandle table, Optional<List<List<JdbcColumnHandle>>> groupingSets, List<JdbcColumnHandle> columns, Map<String, ParameterizedExpression> columnExpressions)
    {
        var preparedQuery = super.prepareQuery(session, table, groupingSets, columns, columnExpressions);
        if (table.isNamedRelation()) {
            var namedRelation = table.getRequiredNamedRelation();
            queryToInfo.put(preparedQuery, new QueryEnhancedInfo(namedRelation));
        }
        return preparedQuery;
    }

    WeakHashMap<PreparedQuery, QueryEnhancedInfo> queryToInfo = new WeakHashMap<>();

    @Override
    public Optional<PreparedQuery> implementJoin(ConnectorSession session, JoinType joinType, PreparedQuery leftSource, Map<JdbcColumnHandle, String> leftProjections, PreparedQuery rightSource, Map<JdbcColumnHandle, String> rightProjections, List<ParameterizedExpression> joinConditions, JoinStatistics statistics)
    {
        QueryEnhancedInfo leftInfo = null;
        QueryEnhancedInfo rightInfo = null;
        if (queryToInfo.containsKey(leftSource)) {
            leftInfo = queryToInfo.get(leftSource);
        }
        if (queryToInfo.containsKey(rightSource)) {
            rightInfo = queryToInfo.get(rightSource);
        }
        QueryEnhancedInfo newInfo = new QueryEnhancedInfo(leftInfo, rightInfo, leftProjections, rightProjections);

        Logger log = Logger.get(OracleClient.class);
        var leftParams = leftSource.parameters()
                .stream().map(Object::toString).collect(Collectors.joining(","));
        var rightParams = rightSource.parameters()
                .stream().map(Object::toString).collect(Collectors.joining(","));
        log.info("implementJoin: left -> " + leftSource.query() + " params " + leftParams);
        log.info("implementJoin: right -> " + rightSource.query() + " params " + rightParams);
        Optional<PreparedQuery> result = super.implementJoin(session, joinType, leftSource, leftProjections, rightSource, rightProjections, joinConditions, statistics);
        if (result.isPresent() && !result.isEmpty()) {
            var resultParam = result.get().parameters()
                    .stream().map(Object::toString).collect(Collectors.joining(","));
            log.info("implementJoin result: " + result.get().query() + " params " + resultParam);
        }
        if (result.isPresent()) {
            queryToInfo.put(result.get(), newInfo);
        }
        return result;
    }

    @Override
    public Optional<PreparedQuery> legacyImplementJoin(ConnectorSession session, JoinType joinType, PreparedQuery leftSource, PreparedQuery rightSource, List<JdbcJoinCondition> joinConditions, Map<JdbcColumnHandle, String> rightAssignments, Map<JdbcColumnHandle, String> leftAssignments, JoinStatistics statistics)
    {
        QueryEnhancedInfo leftInfo = null;
        QueryEnhancedInfo rightInfo = null;
        if (queryToInfo.containsKey(leftSource)) {
            leftInfo = queryToInfo.get(leftSource);
        }
        if (queryToInfo.containsKey(rightSource)) {
            rightInfo = queryToInfo.get(rightSource);
        }
        QueryEnhancedInfo newInfo = new QueryEnhancedInfo(leftInfo, rightInfo, rightAssignments, leftAssignments);
        Logger log = Logger.get(OracleClient.class);
        var leftParams = leftSource.parameters()
                .stream().map(Object::toString).collect(Collectors.joining(","));
        var rightParams = rightSource.parameters()
                .stream().map(Object::toString).collect(Collectors.joining(","));
        log.info("legacyImplementJoin: left -> " + leftSource.query() + " params " + leftParams);
        log.info("legacyImplementJoin: right -> " + rightSource.toString() + " params " + rightParams);
        Optional<PreparedQuery> result = super.legacyImplementJoin(session, joinType, leftSource, rightSource, joinConditions, rightAssignments, leftAssignments, statistics);
        if (result.isPresent()) {
            var resultParam = result.get().parameters()
                    .stream().map(Object::toString).collect(Collectors.joining(","));
            log.info("legacyImplementJoin result: " + result.get().query() + " params " + resultParam);
        }
        if (result.isPresent()) {
            queryToInfo.put(result.get(), newInfo);
        }
        return result;
    }

    public static class RangeInfo
    {
        private final boolean isInteger;
        private final String expression;
        /**
         * The lower bound of the range (included)
         */
        private final Optional lowerBound;
        /**
         * The upper bound of the range (not include the upperBond itself)
         */
        private final Optional upperBound;

        boolean upperInclusive;

        public RangeInfo(String expression, Optional lowerBound, Optional upperBound)
        {
            this.expression = requireNonNull(expression, "expression is null");
            this.lowerBound = requireNonNull(lowerBound, "lowerBound is null");
            this.upperBound = requireNonNull(upperBound, "upperBound is null");
            if ((lowerBound.isPresent() && (lowerBound.get() instanceof Integer))
                    || (upperBound.isPresent() && (upperBound.get() instanceof Integer))) {
                this.isInteger = true;
            }
            else {
                this.isInteger = false;
            }
        }

        public void setUpperInclusive(boolean b)
        {
            this.upperInclusive = b;
        }

        public RangeInfo withUpperInclusive(boolean b)
        {
            this.upperInclusive = b;
            return this;
        }

        public String getExpression()
        {
            return expression;
        }

        public Optional getLowerBound()
        {
            return lowerBound;
        }

        public Optional getUpperBound()
        {
            return upperBound;
        }

        @Override
        public boolean equals(Object obj)
        {
            if (this == obj) {
                return true;
            }

            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }

            RangeInfo that = (RangeInfo) obj;

            return Objects.equals(this.expression, that.expression)
                    && Objects.equals(this.lowerBound, that.lowerBound)
                    && Objects.equals(this.upperBound, that.upperBound);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(expression, lowerBound, upperBound);
        }

        @Override
        public String toString()
        {
            return "[" + lowerBound + ", " + upperBound + ")";
        }

        public boolean isUpperInclusive()
        {
            return this.upperInclusive;
        }

        public boolean isInteger()
        {
            return this.isInteger;
        }
    }
}
