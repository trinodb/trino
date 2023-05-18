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
package io.trino.plugin.neo4j;

import com.google.common.cache.Cache;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.UncheckedExecutionException;
import io.airlift.log.Logger;
import io.trino.collect.cache.EvictableCacheBuilder;
import io.trino.plugin.jdbc.BaseJdbcClient;
import io.trino.plugin.jdbc.BaseJdbcConfig;
import io.trino.plugin.jdbc.ColumnMapping;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.JdbcColumnHandle;
import io.trino.plugin.jdbc.JdbcQueryRelationHandle;
import io.trino.plugin.jdbc.JdbcTableHandle;
import io.trino.plugin.jdbc.JdbcTypeHandle;
import io.trino.plugin.jdbc.LongReadFunction;
import io.trino.plugin.jdbc.LongWriteFunction;
import io.trino.plugin.jdbc.ObjectReadFunction;
import io.trino.plugin.jdbc.ObjectWriteFunction;
import io.trino.plugin.jdbc.PreparedQuery;
import io.trino.plugin.jdbc.QueryBuilder;
import io.trino.plugin.jdbc.StandardColumnMappings;
import io.trino.plugin.jdbc.WriteMapping;
import io.trino.plugin.jdbc.logging.RemoteQueryModifier;
import io.trino.plugin.jdbc.mapping.IdentifierMapping;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.DateTimeEncoding;
import io.trino.spi.type.LongTimeWithTimeZone;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.StandardTypes;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TimeWithTimeZoneType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.TypeSignature;
import org.neo4j.jdbc.bolt.BoltNeo4jConnection;
import org.neo4j.jdbc.bolt.BoltNeo4jResultSetMetaData;
import org.neo4j.jdbc.utils.BoltNeo4jUtils;

import javax.inject.Inject;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.time.LocalDate;
import java.time.OffsetTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.ExecutionException;

import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Throwables.throwIfInstanceOf;
import static com.google.common.base.Verify.verify;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.plugin.base.util.JsonTypeUtil.jsonParse;
import static io.trino.plugin.jdbc.CaseSensitivity.CASE_INSENSITIVE;
import static io.trino.plugin.jdbc.CaseSensitivity.CASE_SENSITIVE;
import static io.trino.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static io.trino.plugin.jdbc.PredicatePushdownController.DISABLE_PUSHDOWN;
import static io.trino.plugin.jdbc.StandardColumnMappings.booleanWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.dateReadFunctionUsingLocalDate;
import static io.trino.plugin.jdbc.StandardColumnMappings.dateWriteFunctionUsingLocalDate;
import static io.trino.plugin.jdbc.StandardColumnMappings.doubleWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.longTimestampReadFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.longTimestampWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.timeReadFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.timeWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.varcharReadFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.varcharWriteFunction;
import static io.trino.plugin.jdbc.TypeHandlingJdbcSessionProperties.getUnsupportedTypeHandling;
import static io.trino.plugin.jdbc.UnsupportedTypeHandling.CONVERT_TO_VARCHAR;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.TimeZoneKey.getTimeZoneKey;
import static io.trino.spi.type.TimestampType.TIMESTAMP_NANOS;
import static io.trino.spi.type.Timestamps.NANOSECONDS_PER_DAY;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_DAY;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_NANOSECOND;
import static io.trino.spi.type.Timestamps.round;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class Neo4jClient
        extends BaseJdbcClient
{
    private static final Logger log = Logger.get(Neo4jClient.class);
    private static final int MAX_RESULT_SET_INFO_CACHE_ENTRIES = 10000;
    private final Type jsonType;
    private final Cache<PreparedQuery, Neo4jResultSetInfo> cachedResultSetInfo;

    @Inject
    public Neo4jClient(BaseJdbcConfig config, ConnectionFactory connectionFactory, QueryBuilder queryBuilder, IdentifierMapping identifierMapping, TypeManager typeManager, RemoteQueryModifier remoteQueryModifier)
    {
        super(config, "`", connectionFactory, queryBuilder, identifierMapping, remoteQueryModifier);
        this.jsonType = typeManager.getType(new TypeSignature(StandardTypes.JSON));
        cachedResultSetInfo = EvictableCacheBuilder.newBuilder()
                .maximumSize(MAX_RESULT_SET_INFO_CACHE_ENTRIES)
                .shareNothingWhenDisabled()
                .recordStats()
                .build();
    }

    @Override
    public Optional<ColumnMapping> toColumnMapping(ConnectorSession session, Connection connection, JdbcTypeHandle typeHandle)
    {
        Optional<ColumnMapping> mapping = getForcedMappingToVarchar(typeHandle);
        if (mapping.isPresent()) {
            return mapping;
        }
        switch (typeHandle.getJdbcType()) {
            case Types.VARCHAR:
            case Types.JAVA_OBJECT:
            case Types.ARRAY:
            case Types.NULL:
                return Optional.of(nullColumnMapping());
            case Types.INTEGER:
                return Optional.of(longColumnMapping());
            case Types.BOOLEAN:
                return Optional.of(booleanColumnMapping());
            case Types.FLOAT:
                return Optional.of(doubleColumnMapping());
            case Types.DATE:
                return Optional.of(dateColumnMapping());
            case Types.TIME:
                if (typeHandle.getJdbcTypeName().get().equals("TIME")) {
                    return Optional.of(offsetTimeColumnMappingShort());
                }
                else {
                    return Optional.of(localTimeColumnMapping());
                }
            case Types.TIMESTAMP:
                return Optional.of(localTimestampColumnMapping());
            case Types.TIMESTAMP_WITH_TIMEZONE:
                return Optional.of(timestampWithTimeZoneColumnMapping());
        }
        if (getUnsupportedTypeHandling(session) == CONVERT_TO_VARCHAR) {
            return mapToUnboundedVarchar(typeHandle);
        }
        return Optional.empty();
    }

    private ColumnMapping varcharColumnMapping()
    {
        return ColumnMapping.sliceMapping(VARCHAR, varcharReadFunction(VARCHAR), varcharWriteFunction(), DISABLE_PUSHDOWN);
    }

    private ColumnMapping longColumnMapping()
    {
        return ColumnMapping.longMapping(BIGINT, ResultSet::getLong, LongWriteFunction.of(Types.INTEGER, (statement, index, value) -> statement.setLong(index, value)), DISABLE_PUSHDOWN);
    }

    private ColumnMapping booleanColumnMapping()
    {
        return ColumnMapping.booleanMapping(BOOLEAN, ResultSet::getBoolean, booleanWriteFunction(), DISABLE_PUSHDOWN);
    }

    private ColumnMapping doubleColumnMapping()
    {
        return ColumnMapping.doubleMapping(DOUBLE, ResultSet::getDouble, doubleWriteFunction(), DISABLE_PUSHDOWN);
    }

    private ColumnMapping dateColumnMapping()
    {
        return ColumnMapping.longMapping(DATE,
                dateReadFunctionUsingLocalDate(),
                dateWriteFunctionUsingLocalDate(), DISABLE_PUSHDOWN);
    }

    /**
     * Initially thought of mapping null column types to an empty slice
     * But neo4j can have mixed types for the same column, if a column of the first row in results set is null, the column is designated
     * as jdbc NULL type in ResultSetMetadata even though rest of the rows could have valid values
     * So, we cannot map NULL types to an empty slice instead force the type to Unbounded Varchar
     * @return
     */
    private ColumnMapping nullColumnMapping()
    {
//        return ColumnMapping.sliceMapping(VARCHAR, (resultSet, columnIndex) -> EMPTY_SLICE, varcharWriteFunction(), DISABLE_PUSHDOWN);
        return varcharColumnMapping();
    }

    private ColumnMapping localTimeColumnMapping()
    {
        TimeType timeType = TimeType.TIME_NANOS;
        return ColumnMapping.longMapping(
                timeType,
                timeReadFunction(timeType),
                timeWriteFunction(timeType.getPrecision()), DISABLE_PUSHDOWN);
    }

    private ColumnMapping localTimestampColumnMapping()
    {
        TimestampType timestampType = TIMESTAMP_NANOS;
        return ColumnMapping.objectMapping(
                timestampType,
                longTimestampReadFunction(timestampType),
                longTimestampWriteFunction(timestampType, timestampType.getPrecision()),
                DISABLE_PUSHDOWN);
    }

    private ColumnMapping offsetTimeColumnMappingShort()
    {
        TimeType timeType = TimeType.TIME_NANOS;
        return ColumnMapping.longMapping(
                TimeWithTimeZoneType.createTimeWithTimeZoneType(timeType.getPrecision()),
                offsetTimeReadFunctionShort(timeType),
                StandardColumnMappings.timeWriteFunction(timeType.getPrecision()), DISABLE_PUSHDOWN);
    }

    private ColumnMapping offsetTimeColumnMappingLong()
    {
        TimeType timeType = TimeType.TIME_PICOS;
        return ColumnMapping.objectMapping(
                timeType,
                offsetTimeReadFunctionLong(timeType),
                dummyObjectWriteFunction(LongTimeWithTimeZone.class));
    }

    /**
     * Since this connector doesn't support Writes yet, using this for creating a dummy write function
     * @param classType
     * @return
     */
    private ObjectWriteFunction dummyObjectWriteFunction(Class classType)
    {
        return ObjectWriteFunction.of(
                classType,
                (statement, index, value) -> {
                    // do nothing
                });
    }

    private ObjectReadFunction offsetTimeReadFunctionLong(TimeType timeType)
    {
        requireNonNull(timeType, "timeType is null");
        checkArgument(timeType.getPrecision() <= 9, "Unsupported type precision: %s", timeType);
        return ObjectReadFunction.of(
                LongTimeWithTimeZone.class,
                (resultSet, columnIndex) -> {
                    OffsetTime time = resultSet.getObject(columnIndex, OffsetTime.class);
                    long epochSeconds = time.toEpochSecond(LocalDate.EPOCH);
                    ZoneOffset offset = time.getOffset();
                    long nanosOfDay = (epochSeconds + offset.getTotalSeconds()) * 1000000000L + time.getNano();

                    verify(nanosOfDay < NANOSECONDS_PER_DAY, "Invalid value of nanosOfDay: %s", nanosOfDay);
                    long picosOfDay = nanosOfDay * PICOSECONDS_PER_NANOSECOND;
                    long rounded = round(picosOfDay, 12 - timeType.getPrecision());
                    if (rounded == PICOSECONDS_PER_DAY) {
                        rounded = 0;
                    }
                    return new LongTimeWithTimeZone(picosOfDay, offset.getTotalSeconds() / 60);
                });
    }

    public static LongReadFunction offsetTimeReadFunctionShort(TimeType timeType)
    {
        requireNonNull(timeType, "timeType is null");
        checkArgument(timeType.getPrecision() <= 9, "Unsupported type precision: %s", timeType);
        return (resultSet, columnIndex) -> {
            OffsetTime time = resultSet.getObject(columnIndex, OffsetTime.class);
            long epochSeconds = time.toEpochSecond(LocalDate.EPOCH);
            ZoneOffset offset = time.getOffset();
            long nanosOfDay = (epochSeconds + offset.getTotalSeconds()) * 1000000000L + time.getNano();
            return DateTimeEncoding.packTimeWithTimeZone(nanosOfDay, offset.getTotalSeconds() / 60);
        };
    }

    private ColumnMapping timestampWithTimeZoneColumnMapping()
    {
        TimestampWithTimeZoneType trinoType = TimestampWithTimeZoneType.TIMESTAMP_TZ_NANOS;
        return ColumnMapping.objectMapping(
                trinoType,
                longTimestampWithTimeZoneReadFunction(),
                dummyObjectWriteFunction(LongTimestampWithTimeZone.class), DISABLE_PUSHDOWN);
    }

    private ObjectReadFunction longTimestampWithTimeZoneReadFunction()
    {
        return ObjectReadFunction.of(
                LongTimestampWithTimeZone.class,
                (resultSet, columnIndex) -> {
                    ZonedDateTime zonedDateTime = resultSet.getObject(columnIndex, ZonedDateTime.class);
                    return LongTimestampWithTimeZone.fromEpochSecondsAndFraction(
                            zonedDateTime.toEpochSecond(),
                            (long) zonedDateTime.getNano() * PICOSECONDS_PER_NANOSECOND,
                            getTimeZoneKey(zonedDateTime.getZone().toString()));
                });
    }

    @Override
    public JdbcTableHandle getTableHandle(ConnectorSession session, PreparedQuery preparedQuery)
    {
        ImmutableList.Builder<JdbcColumnHandle> columns = ImmutableList.builder();
        try (Connection connection = connectionFactory.openConnection(session)) {
            ResultSetMetaData metadata = this.getColumnMetadata(connection, preparedQuery);
            if (metadata == null) {
                throw new UnsupportedOperationException("Query not supported: ResultSetMetaData not available for query: " + preparedQuery.getQuery());
            }
            for (int column = 1; column <= metadata.getColumnCount(); column++) {
                String name = metadata.getColumnName(column);
                String typeName = "NULL";
                try {
                    typeName = metadata.getColumnTypeName(column);
                }
                catch (NullPointerException npe) {
                    // there's a bug in neo4j jdbc driver where it throws an exception when the column value is null
                    // Ignore and proceed
                }
                JdbcTypeHandle jdbcTypeHandle = new JdbcTypeHandle(
                        metadata.getColumnType(column),
                        Optional.ofNullable(typeName),
                        Optional.of(metadata.getPrecision(column)),
                        Optional.of(metadata.getScale(column)),
                        Optional.empty(), // TODO support arrays
                        Optional.of(metadata.isCaseSensitive(column) ? CASE_SENSITIVE : CASE_INSENSITIVE));
                Type type = toColumnMapping(session, connection, jdbcTypeHandle)
                        .orElseThrow(() -> new UnsupportedOperationException(format("Unsupported type: %s of column: %s", jdbcTypeHandle, name)))
                        .getType();
                columns.add(new JdbcColumnHandle(name, jdbcTypeHandle, type));
            }
        }
        catch (SQLException ex) {
            throw new TrinoException(JDBC_ERROR, "Failed to get table handle for prepared query. " + firstNonNull(ex.getMessage(), ex), ex);
        }
        catch (Throwable t) {
            if (t instanceof UnsupportedOperationException || t instanceof TrinoException) {
                throw t;
            }
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Failed to get table handle for prepared query. " + firstNonNull(t.getMessage(), t), t);
        }

        return new JdbcTableHandle(
                new JdbcQueryRelationHandle(preparedQuery),
                TupleDomain.all(),
                ImmutableList.of(),
                Optional.empty(),
                OptionalLong.empty(),
                Optional.of(columns.build()),
                // The query is opaque, so we don't know referenced tables
                Optional.empty(),
                0,
                Optional.empty());
    }

    private Neo4jResultSetInfo loadNeo4jResultSetInfo(BoltNeo4jConnection boltNeo4jConnection, PreparedQuery preparedQuery)
    {
        Neo4jResultSetInfo resultSetInfo = new Neo4jResultSetInfo();
        resultSetInfo.setHasResultSet(BoltNeo4jUtils.hasResultSet(boltNeo4jConnection, preparedQuery.getQuery()));
        return resultSetInfo;
    }

    private Neo4jResultSetInfo getNeo4jResultSetInfo(BoltNeo4jConnection boltNeo4jConnection, PreparedQuery preparedQuery)
    {
        try {
            return this.cachedResultSetInfo.get(preparedQuery, () -> loadNeo4jResultSetInfo(boltNeo4jConnection, preparedQuery));
        }
        catch (UncheckedExecutionException e) {
            throwIfInstanceOf(e.getCause(), TrinoException.class);
            throw e;
        }
        catch (ExecutionException e) {
            throwIfInstanceOf(e.getCause(), TrinoException.class);
            throw new UncheckedExecutionException(e);
        }
    }

    private ResultSetMetaData getColumnMetadata(Connection connection, PreparedQuery preparedQuery) throws SQLException
    {
        BoltNeo4jConnection boltNeo4jConnection = connection.unwrap(BoltNeo4jConnection.class);
        Neo4jResultSetInfo resultSetInfo = getNeo4jResultSetInfo(boltNeo4jConnection, preparedQuery);
        if (!resultSetInfo.isHasResultSet()) {
            throw new UnsupportedOperationException("Error get results for the given query. " + preparedQuery.getQuery());
        }
        if (resultSetInfo.getMetadata() == null) {
            try (PreparedStatement mdStatement = connection.prepareStatement(preparedQuery.getQuery());) {
                mdStatement.setMaxRows(10);
                int paramCount = preparedQuery.getParameters().size();
                for (int i = 1; i <= paramCount; i++) {
                    mdStatement.setString(i, null);
                }

                boolean hadResults = mdStatement.execute();
                if (hadResults) {
                    java.sql.ResultSet mdRs = mdStatement.getResultSet();
                    resultSetInfo.setMetadata(mdRs.getMetaData());
                }
                else {
                    List<org.neo4j.driver.types.Type> types = new ArrayList<>();
                    ResultSetMetaData rsmd = BoltNeo4jResultSetMetaData.newInstance(false, types, new ArrayList<>());
                    resultSetInfo.setMetadata(rsmd);
                }
            }
            catch (SQLException e) {
                throw new TrinoException(JDBC_ERROR, "Failed to get metadata for prepared query. " + firstNonNull(e.getMessage(), e), e);
            }
        }
        return resultSetInfo.getMetadata();
    }

    private ColumnMapping jsonColumnMapping()
    {
        return ColumnMapping.sliceMapping(
                jsonType,
                (resultSet, columnIndex) -> jsonParse(utf8Slice(resultSet.getString(columnIndex))),
                varcharWriteFunction(),
                DISABLE_PUSHDOWN);
    }

    @Override
    public WriteMapping toWriteMapping(ConnectorSession session, Type type)
    {
        return null;
    }

    public Cache<PreparedQuery, Neo4jResultSetInfo> getCachedResultSetInfo()
    {
        return cachedResultSetInfo;
    }
}
