/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugins.snowflake.parallel;

import com.google.common.base.VerifyException;
import com.starburstdata.trino.plugins.snowflake.jdbc.SnowflakeClient;
import io.airlift.slice.Slice;
import io.trino.plugin.jdbc.BooleanWriteFunction;
import io.trino.plugin.jdbc.ColumnMapping;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.DoubleWriteFunction;
import io.trino.plugin.jdbc.JdbcClient;
import io.trino.plugin.jdbc.JdbcColumnHandle;
import io.trino.plugin.jdbc.JdbcProcedureHandle;
import io.trino.plugin.jdbc.JdbcSplitManager;
import io.trino.plugin.jdbc.JdbcTableHandle;
import io.trino.plugin.jdbc.JdbcTypeHandle;
import io.trino.plugin.jdbc.LongWriteFunction;
import io.trino.plugin.jdbc.ObjectWriteFunction;
import io.trino.plugin.jdbc.PreparedQuery;
import io.trino.plugin.jdbc.QueryParameter;
import io.trino.plugin.jdbc.SliceWriteFunction;
import io.trino.plugin.jdbc.WriteFunction;
import io.trino.plugin.jdbc.logging.RemoteQueryModifier;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.FixedSplitSource;
import io.trino.spi.type.Type;
import net.snowflake.client.core.ParameterBindingDTO;
import net.snowflake.client.core.SFException;
import net.snowflake.client.core.SFStatement;
import net.snowflake.client.core.SessionUtil;
import net.snowflake.client.jdbc.SnowflakeConnectionV1;
import net.snowflake.client.jdbc.StarburstSnowflakeStatementV1;
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.JsonNode;

import javax.inject.Inject;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Verify.verify;
import static com.starburstdata.trino.plugins.snowflake.parallel.SnowflakeArrowSplit.newChunkFileSplit;
import static com.starburstdata.trino.plugins.snowflake.parallel.SnowflakeArrowSplit.newEncodedSplit;
import static com.starburstdata.trino.plugins.snowflake.parallel.SnowflakeParallelSessionProperties.getFullyParallelModeEnabled;
import static io.trino.plugin.jdbc.JdbcDynamicFilteringSessionProperties.dynamicFilteringEnabled;
import static io.trino.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static net.snowflake.client.core.ResultUtil.effectiveParamValue;

public class SnowflakeSplitManager
        implements ConnectorSplitManager
{
    private final ConnectionFactory connectionFactory;
    private final SnowflakeClient snowflakeClient;
    private final RemoteQueryModifier queryModifier;
    private final JdbcSplitManager jdbcSplitManager;

    @Inject
    public SnowflakeSplitManager(
            ConnectionFactory connectionFactory,
            SnowflakeClient snowflakeClient,
            RemoteQueryModifier queryModifier,
            JdbcSplitManager jdbcSplitManager)
    {
        this.connectionFactory = requireNonNull(connectionFactory, "connectionFactory is null");
        this.snowflakeClient = requireNonNull(snowflakeClient, "snowflakeClient is null");
        this.queryModifier = requireNonNull(queryModifier, "queryModifier is null");
        this.jdbcSplitManager = requireNonNull(jdbcSplitManager, "jdbcSplitManager is null");
    }

    @Override
    public ConnectorSplitSource getSplits(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorTableHandle table,
            DynamicFilter dynamicFilter,
            Constraint constraint)
    {
        if (table instanceof JdbcProcedureHandle) {
            return jdbcSplitManager.getSplits(transaction, session, table, dynamicFilter, constraint);
        }
        JdbcTableHandle jdbcTableHandle = (JdbcTableHandle) table;
        // push down synthetic table handles to JDBC code path, also sort oder can't be preserved in fully parallel mode
        if (!getFullyParallelModeEnabled(session) && (!jdbcTableHandle.isNamedRelation() || jdbcTableHandle.getSortOrder().isPresent())) {
            return jdbcSplitManager.getSplits(transaction, session, table, dynamicFilter, constraint);
        }
        try (Connection connection = connectionFactory.openConnection(session)) {
            List<JdbcColumnHandle> columns = jdbcTableHandle.getColumns()
                    .map(columnSet -> columnSet.stream().map(JdbcColumnHandle.class::cast).collect(toList()))
                    .orElseGet(() -> snowflakeClient.getColumns(session, jdbcTableHandle));

            PreparedQuery preparedQuery = snowflakeClient.prepareQuery(
                    session,
                    connection,
                    dynamicFilteringEnabled(session) ? jdbcTableHandle.intersectedWithConstraint(dynamicFilter.getCurrentPredicate()) : jdbcTableHandle,
                    columns,
                    Optional.empty());

            Map<String, ParameterBindingDTO> bindValues = convertToSnowflakeFormatWithStatement(preparedQuery, session, connection);
            SFStatement sFStatement = new SFStatement(connection.unwrap(SnowflakeConnectionV1.class).getSfSession());
            JsonNode jsonResult = (JsonNode) sFStatement.executeHelper(
                    preparedQuery.getQuery(),
                    "application/snowflake",
                    bindValues,
                    false,
                    false,
                    false);

            return new FixedSplitSource(parseChunkFiles(jsonResult));
        }
        catch (SFException | SQLException e) {
            throw new TrinoException(JDBC_ERROR, "Couldn't get Snowflake splits", e);
        }
    }

    /**
     * This is a duplication of {@link io.trino.plugin.jdbc.DefaultQueryBuilder}
     * Potential Trino change: QueryBuilder.prepareStatement(client, ...) -> QueryBuilder.prepareStatement(statement, ...)
     * would fix the duplication, but it won't be consistent with remaining class methods
     */
    public Map<String, ParameterBindingDTO> convertToSnowflakeFormatWithStatement(PreparedQuery preparedQuery, ConnectorSession session, Connection connection)
            throws SQLException
    {
        String modifiedQuery = queryModifier.apply(session, preparedQuery.getQuery());
        StarburstSnowflakeStatementV1 statement = new StarburstSnowflakeStatementV1(connection.unwrap(SnowflakeConnectionV1.class), modifiedQuery);

        List<QueryParameter> parameters = preparedQuery.getParameters();
        for (int i = 0; i < parameters.size(); i++) {
            QueryParameter parameter = parameters.get(i);
            int parameterIndex = i + 1;
            WriteFunction writeFunction = parameter.getJdbcType()
                    .map(jdbcType -> getWriteFunction(snowflakeClient, session, connection, jdbcType, parameter.getType()))
                    .orElseGet(() -> getWriteFunction(snowflakeClient, session, parameter.getType()));
            Class<?> javaType = writeFunction.getJavaType();
            Object value = parameter.getValue()
                    // The value must be present, since DefaultQueryBuilder never creates null parameters. Values coming from Domain's ValueSet are non-null, and
                    // nullable domains are handled explicitly, with SQL syntax.
                    .orElseThrow(() -> new VerifyException("Value is missing"));
            if (javaType == boolean.class) {
                ((BooleanWriteFunction) writeFunction).set(statement, parameterIndex, (boolean) value);
            }
            else if (javaType == long.class) {
                ((LongWriteFunction) writeFunction).set(statement, parameterIndex, (long) value);
            }
            else if (javaType == double.class) {
                ((DoubleWriteFunction) writeFunction).set(statement, parameterIndex, (double) value);
            }
            else if (javaType == Slice.class) {
                ((SliceWriteFunction) writeFunction).set(statement, parameterIndex, (Slice) value);
            }
            else {
                ((ObjectWriteFunction) writeFunction).set(statement, parameterIndex, value);
            }
        }

        return statement.getParameterBindings();
    }

    private static WriteFunction getWriteFunction(JdbcClient client, ConnectorSession session, Connection connection, JdbcTypeHandle jdbcTypeHandle, Type type)
    {
        ColumnMapping columnMapping = client.toColumnMapping(session, connection, jdbcTypeHandle)
                .orElseThrow(() -> new VerifyException(format("Unsupported type %s with handle %s", type, jdbcTypeHandle)));
        WriteFunction writeFunction = columnMapping.getWriteFunction();
        verify(writeFunction.getJavaType() == type.getJavaType(), "Java type mismatch: %s, %s", writeFunction, type);
        return writeFunction;
    }

    private static WriteFunction getWriteFunction(JdbcClient client, ConnectorSession session, Type type)
    {
        return client.toWriteMapping(session, type).getWriteFunction();
    }

    /**
     * Originates from {@link net.snowflake.client.jdbc.SnowflakeResultSetSerializableV1#parseChunkFiles()}
     */
    private List<ConnectorSplit> parseChunkFiles(JsonNode rootNode)
    {
        JsonNode data = rootNode.path("data");
        JsonNode chunksNode = data.path("chunks");

        // try to get the Query Result Master Key
        JsonNode qrmkNode = data.path("qrmk");
        String qrmk = qrmkNode.isMissingNode() ? null : qrmkNode.textValue();

        // parse chunk headers
        Map<String, String> chunkHeadersMap = new HashMap<>();
        JsonNode chunkHeaders = data.path("chunkHeaders");
        if (chunkHeaders != null && !chunkHeaders.isMissingNode()) {
            Iterator<Map.Entry<String, JsonNode>> chunkHeadersIter = chunkHeaders.fields();
            while (chunkHeadersIter.hasNext()) {
                Map.Entry<String, JsonNode> chunkHeader = chunkHeadersIter.next();
                chunkHeadersMap.put(chunkHeader.getKey(), chunkHeader.getValue().asText());
            }
        }

        SnowflakeSessionParameters parameters = parseParameters(SessionUtil.getCommonParams(data.path("parameters")));
        long resultVersion = !data.path("version").isMissingNode() ? data.path("version").longValue() : 0;

        // we will encounter both chunks and rowset value at the same time, or just the rowset value for small queries
        JsonNode rowsetBase64 = data.path("rowsetBase64");
        boolean encodedChunkIsPresentInJson = !rowsetBase64.isMissingNode() && !rowsetBase64.asText("").isBlank();

        int chunkFileCount = chunksNode.size();
        List<ConnectorSplit> splits = new ArrayList<>(encodedChunkIsPresentInJson ? chunkFileCount + 1 : chunkFileCount);

        if (encodedChunkIsPresentInJson) {
            splits.add(newEncodedSplit(rowsetBase64.asText(), parameters, resultVersion));
        }

        // parse chunk files metadata, e.g. url and row count
        for (int index = 0; index < chunkFileCount; index++) {
            JsonNode chunkNode = chunksNode.get(index);
            String url = chunkNode.path("url").asText();
            int rowCount = chunkNode.path("rowCount").asInt();
            int compressedSize = chunkNode.path("compressedSize").asInt();
            int uncompressedSize = chunkNode.path("uncompressedSize").asInt();

            splits.add(newChunkFileSplit(url, uncompressedSize, compressedSize, rowCount, chunkHeadersMap, parameters, qrmk, resultVersion));
        }
        return splits;
    }

    private static SnowflakeSessionParameters parseParameters(Map<String, Object> parameters)
    {
        return new SnowflakeSessionParameters(
                (String) effectiveParamValue(parameters, "TIMESTAMP_OUTPUT_FORMAT"),
                (String) effectiveParamValue(parameters, "TIMESTAMP_NTZ_OUTPUT_FORMAT"),
                (String) effectiveParamValue(parameters, "TIMESTAMP_LTZ_OUTPUT_FORMAT"),
                (String) effectiveParamValue(parameters, "TIMESTAMP_TZ_OUTPUT_FORMAT"),
                (String) effectiveParamValue(parameters, "DATE_OUTPUT_FORMAT"),
                (String) effectiveParamValue(parameters, "TIME_OUTPUT_FORMAT"),
                (String) effectiveParamValue(parameters, "TIMEZONE"),
                (boolean) effectiveParamValue(parameters, "CLIENT_HONOR_CLIENT_TZ_FOR_TIMESTAMP_NTZ"),
                (String) effectiveParamValue(parameters, "BINARY_OUTPUT_FORMAT"));
    }
}
