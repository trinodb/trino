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
package io.trino.server.protocol;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import io.airlift.log.Logger;
import io.trino.client.ClientTypeSignature;
import io.trino.client.ClientTypeSignatureParameter;
import io.trino.client.Column;
import io.trino.client.FailureInfo;
import io.trino.client.NamedClientTypeSignature;
import io.trino.client.QueryError;
import io.trino.client.RowFieldName;
import io.trino.client.StageStats;
import io.trino.client.StatementStats;
import io.trino.client.Warning;
import io.trino.execution.ExecutionFailureInfo;
import io.trino.execution.QueryInfo;
import io.trino.execution.QueryState;
import io.trino.execution.QueryStats;
import io.trino.execution.StageInfo;
import io.trino.execution.TaskInfo;
import io.trino.spi.ErrorCode;
import io.trino.spi.TrinoWarning;
import io.trino.spi.WarningCode;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeSignature;
import io.trino.spi.type.TypeSignatureParameter;
import io.trino.sql.ExpressionFormatter;
import io.trino.sql.analyzer.TypeSignatureTranslator;
import io.trino.sql.tree.DataType;
import io.trino.sql.tree.DateTimeDataType;
import io.trino.sql.tree.GenericDataType;
import io.trino.sql.tree.IntervalDayTimeDataType;
import io.trino.sql.tree.NumericParameter;
import io.trino.sql.tree.RowDataType;
import io.trino.sql.tree.TypeParameter;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.execution.QueryState.FAILED;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.type.StandardTypes.ROW;
import static io.trino.spi.type.StandardTypes.TIME;
import static io.trino.spi.type.StandardTypes.TIMESTAMP;
import static io.trino.spi.type.StandardTypes.TIMESTAMP_WITH_TIME_ZONE;
import static io.trino.spi.type.StandardTypes.TIME_WITH_TIME_ZONE;
import static io.trino.util.Failures.toFailure;
import static java.lang.String.format;

public final class ProtocolUtil
{
    private static final Logger log = Logger.get(ProtocolUtil.class);

    private ProtocolUtil() {}

    public static Column createColumn(String name, Type type, boolean supportsParametricDateTime)
    {
        String formatted = formatType(TypeSignatureTranslator.toSqlType(type), supportsParametricDateTime);

        return new Column(name, formatted, toClientTypeSignature(type.getTypeSignature(), supportsParametricDateTime));
    }

    private static String formatType(DataType type, boolean supportsParametricDateTime)
    {
        if (type instanceof DateTimeDataType dataTimeType) {
            if (!supportsParametricDateTime) {
                if (dataTimeType.getType() == DateTimeDataType.Type.TIMESTAMP && dataTimeType.isWithTimeZone()) {
                    return TIMESTAMP_WITH_TIME_ZONE;
                }
                if (dataTimeType.getType() == DateTimeDataType.Type.TIMESTAMP && !dataTimeType.isWithTimeZone()) {
                    return TIMESTAMP;
                }
                if (dataTimeType.getType() == DateTimeDataType.Type.TIME && !dataTimeType.isWithTimeZone()) {
                    return TIME;
                }
                if (dataTimeType.getType() == DateTimeDataType.Type.TIME && dataTimeType.isWithTimeZone()) {
                    return TIME_WITH_TIME_ZONE;
                }
            }

            return ExpressionFormatter.formatExpression(type);
        }
        if (type instanceof RowDataType rowDataType) {
            return rowDataType.getFields().stream()
                    .map(field -> field.getName().map(name -> name + " ").orElse("") + formatType(field.getType(), supportsParametricDateTime))
                    .collect(Collectors.joining(", ", ROW + "(", ")"));
        }
        if (type instanceof GenericDataType dataType) {
            if (dataType.getArguments().isEmpty()) {
                return dataType.getName().getValue();
            }

            return dataType.getArguments().stream()
                    .map(parameter -> {
                        if (parameter instanceof NumericParameter) {
                            return ((NumericParameter) parameter).getValue();
                        }
                        if (parameter instanceof TypeParameter) {
                            return formatType(((TypeParameter) parameter).getValue(), supportsParametricDateTime);
                        }
                        throw new IllegalArgumentException("Unsupported parameter type: " + parameter.getClass().getName());
                    })
                    .collect(Collectors.joining(", ", dataType.getName().getValue() + "(", ")"));
        }
        if (type instanceof IntervalDayTimeDataType) {
            return ExpressionFormatter.formatExpression(type);
        }

        throw new IllegalArgumentException("Unsupported data type: " + type.getClass().getName());
    }

    private static ClientTypeSignature toClientTypeSignature(TypeSignature signature, boolean supportsParametricDateTime)
    {
        if (!supportsParametricDateTime) {
            if (signature.getBase().equalsIgnoreCase(TIMESTAMP)) {
                return new ClientTypeSignature(TIMESTAMP);
            }
            if (signature.getBase().equalsIgnoreCase(TIMESTAMP_WITH_TIME_ZONE)) {
                return new ClientTypeSignature(TIMESTAMP_WITH_TIME_ZONE);
            }
            if (signature.getBase().equalsIgnoreCase(TIME)) {
                return new ClientTypeSignature(TIME);
            }
            if (signature.getBase().equalsIgnoreCase(TIME_WITH_TIME_ZONE)) {
                return new ClientTypeSignature(TIME_WITH_TIME_ZONE);
            }
        }

        return new ClientTypeSignature(signature.getBase(), signature.getParameters().stream()
                .map(parameter -> toClientTypeSignatureParameter(parameter, supportsParametricDateTime))
                .collect(toImmutableList()));
    }

    private static ClientTypeSignatureParameter toClientTypeSignatureParameter(TypeSignatureParameter parameter, boolean supportsParametricDateTime)
    {
        switch (parameter.getKind()) {
            case TYPE:
                return ClientTypeSignatureParameter.ofType(toClientTypeSignature(parameter.getTypeSignature(), supportsParametricDateTime));
            case NAMED_TYPE:
                return ClientTypeSignatureParameter.ofNamedType(new NamedClientTypeSignature(
                        parameter.getNamedTypeSignature().getFieldName().map(value ->
                                new RowFieldName(value.getName())),
                        toClientTypeSignature(parameter.getNamedTypeSignature().getTypeSignature(), supportsParametricDateTime)));
            case LONG:
                return ClientTypeSignatureParameter.ofLong(parameter.getLongLiteral());
            case VARIABLE:
                // not expected here
        }
        throw new IllegalArgumentException("Unsupported kind: " + parameter.getKind());
    }

    public static StatementStats toStatementStats(QueryInfo queryInfo)
    {
        QueryStats queryStats = queryInfo.getQueryStats();
        StageInfo outputStage = queryInfo.getOutputStage().orElse(null);

        Set<String> globalUniqueNodes = new HashSet<>();
        StageStats rootStageStats = toStageStats(outputStage, globalUniqueNodes);

        return StatementStats.builder()
                .setState(queryInfo.getState().toString())
                .setQueued(queryInfo.getState() == QueryState.QUEUED)
                .setScheduled(queryInfo.isScheduled())
                .setNodes(globalUniqueNodes.size())
                .setTotalSplits(queryStats.getTotalDrivers())
                .setQueuedSplits(queryStats.getQueuedDrivers())
                .setRunningSplits(queryStats.getRunningDrivers() + queryStats.getBlockedDrivers())
                .setCompletedSplits(queryStats.getCompletedDrivers())
                .setCpuTimeMillis(queryStats.getTotalCpuTime().toMillis())
                .setWallTimeMillis(queryStats.getTotalScheduledTime().toMillis())
                .setQueuedTimeMillis(queryStats.getQueuedTime().toMillis())
                .setElapsedTimeMillis(queryStats.getElapsedTime().toMillis())
                .setProcessedRows(queryStats.getRawInputPositions())
                .setProcessedBytes(queryStats.getRawInputDataSize().toBytes())
                .setPhysicalInputBytes(queryStats.getPhysicalInputDataSize().toBytes())
                .setPeakMemoryBytes(queryStats.getPeakUserMemoryReservation().toBytes())
                .setSpilledBytes(queryStats.getSpilledDataSize().toBytes())
                .setRootStage(rootStageStats)
                .build();
    }

    private static StageStats toStageStats(StageInfo stageInfo, Set<String> globalUniqueNodes)
    {
        if (stageInfo == null) {
            return null;
        }

        io.trino.execution.StageStats stageStats = stageInfo.getStageStats();

        // Store current stage details into a builder
        StageStats.Builder builder = StageStats.builder()
                .setStageId(String.valueOf(stageInfo.getStageId().getId()))
                .setState(stageInfo.getState().toString())
                .setDone(stageInfo.getState().isDone())
                .setTotalSplits(stageStats.getTotalDrivers())
                .setQueuedSplits(stageStats.getQueuedDrivers())
                .setRunningSplits(stageStats.getRunningDrivers() + stageStats.getBlockedDrivers())
                .setCompletedSplits(stageStats.getCompletedDrivers())
                .setCpuTimeMillis(stageStats.getTotalCpuTime().toMillis())
                .setWallTimeMillis(stageStats.getTotalScheduledTime().toMillis())
                .setProcessedRows(stageStats.getRawInputPositions())
                .setProcessedBytes(stageStats.getRawInputDataSize().toBytes())
                .setPhysicalInputBytes(stageStats.getPhysicalInputDataSize().toBytes())
                .setFailedTasks(stageStats.getFailedTasks())
                .setCoordinatorOnly(stageInfo.isCoordinatorOnly())
                .setNodes(countStageAndAddGlobalUniqueNodes(stageInfo, globalUniqueNodes));

        // Recurse into child stages to create their StageStats
        List<StageInfo> subStages = stageInfo.getSubStages();
        if (subStages.isEmpty()) {
            builder.setSubStages(ImmutableList.of());
        }
        else {
            ImmutableList.Builder<StageStats> subStagesBuilder = ImmutableList.builderWithExpectedSize(subStages.size());
            for (StageInfo subStage : subStages) {
                subStagesBuilder.add(toStageStats(subStage, globalUniqueNodes));
            }
            builder.setSubStages(subStagesBuilder.build());
        }

        return builder.build();
    }

    private static int countStageAndAddGlobalUniqueNodes(StageInfo stageInfo, Set<String> globalUniqueNodes)
    {
        List<TaskInfo> tasks = stageInfo.getTasks();
        Set<String> stageUniqueNodes = Sets.newHashSetWithExpectedSize(tasks.size());
        for (TaskInfo task : tasks) {
            String nodeId = task.getTaskStatus().getNodeId();
            stageUniqueNodes.add(nodeId);
            globalUniqueNodes.add(nodeId);
        }
        return stageUniqueNodes.size();
    }

    public static Warning toClientWarning(TrinoWarning warning)
    {
        WarningCode code = warning.getWarningCode();
        return new Warning(new Warning.Code(code.getCode(), code.getName()), warning.getMessage());
    }

    public static QueryError toQueryError(QueryInfo queryInfo)
    {
        QueryState state = queryInfo.getState();
        if (state != FAILED) {
            return null;
        }

        ExecutionFailureInfo executionFailure;
        if (queryInfo.getFailureInfo() != null) {
            executionFailure = queryInfo.getFailureInfo();
        }
        else {
            log.warn("Query %s in state %s has no failure info", queryInfo.getQueryId(), state);
            executionFailure = toFailure(new RuntimeException(format("Query is %s (reason unknown)", state)));
        }
        FailureInfo failure = executionFailure.toFailureInfo();

        ErrorCode errorCode;
        if (queryInfo.getErrorCode() != null) {
            errorCode = queryInfo.getErrorCode();
        }
        else {
            errorCode = GENERIC_INTERNAL_ERROR.toErrorCode();
            log.warn("Failed query %s has no error code", queryInfo.getQueryId());
        }
        return new QueryError(
                firstNonNull(failure.getMessage(), "Internal error"),
                null,
                errorCode.getCode(),
                errorCode.getName(),
                errorCode.getType().toString(),
                failure.getErrorLocation(),
                failure);
    }
}
