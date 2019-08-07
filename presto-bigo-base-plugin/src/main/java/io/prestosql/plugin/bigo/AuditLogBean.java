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
package io.prestosql.plugin.bigo;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.google.common.base.CaseFormat;
import io.prestosql.spi.PrestoWarning;
import io.prestosql.spi.eventlistener.*;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.lang.reflect.Field;
import java.time.Duration;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Optional;

/**
 * @author tangyun@bigo.sg
 * @date 7/2/19 11:08 AM
 */
@Data
@Slf4j
public class AuditLogBean
{
    // from metadata
    private String queryId;
    private String transactionId;
    private String query;
    private String preparedQuery;
    private String queryState;
    private String uri;

    // from statistics
    private Duration cpuTime;
    private Duration wallTime;
    private Duration queuedTime;
    private Duration waitingTime;
    private Duration analysisTime;
    private Duration distributedPlanningTime;
    private long peakUserMemoryBytes;
    // peak of user + system memory
    private long peakTotalNonRevocableMemoryBytes;
    private long peakTaskUserMemory;
    private long peakTaskTotalMemory;
    private long physicalInputBytes;
    private long physicalInputRows;
    private long internalNetworkBytes;
    private long internalNetworkRows;
    private long totalBytes;
    private long totalRows;
    private long outputBytes;
    private long outputRows;
    private long writtenBytes;
    private long writtenRows;
    private double cumulativeMemory;
    private int completedSplits;
    private boolean complete;

    // from context
    private String user;
    private String principal;
    private String remoteClientAddress;
    private String userAgent;
    private String clientInfo;
    private String source;
    private String catalog;
    private String schema;
    private String serverAddress;
    private String serverVersion;
    private String environment;
    private String syntax;
    private String sessionProperties;

    // from failureInfo
    private String failureType;
    private String failureMessage;
    private String failureHost;
    private String failuresJson;

    private String createTime;
    private String executionStartTime;
    private String endTime;

    // from warning
    private List<PrestoWarning> prestoWarnings;

    // from ioMetadata
    private List<QueryInputMetadata> inputs;
    private String catalogName;
    private String outputSchema;
    private String table;


    public AuditLogBean(QueryCompletedEvent queryCompletedEvent)
    {
        QueryMetadata queryMetadata = queryCompletedEvent.getMetadata();
        queryId = queryMetadata.getQueryId();
        transactionId = getFromOptionalString(queryMetadata.getTransactionId());
        query = queryMetadata.getQuery();
        preparedQuery = getFromOptionalString(queryMetadata.getPreparedQuery());
        queryState = queryMetadata.getQueryState();
        uri = queryMetadata.getUri().toString();

        QueryStatistics statistics = queryCompletedEvent.getStatistics();
        cpuTime = statistics.getCpuTime();
        wallTime = statistics.getWallTime();
        queuedTime = statistics.getQueuedTime();
        waitingTime = getFromOptionalDuration(statistics.getResourceWaitingTime());
        analysisTime = getFromOptionalDuration(statistics.getAnalysisTime());
        distributedPlanningTime = getFromOptionalDuration(statistics.getDistributedPlanningTime());
        peakUserMemoryBytes = statistics.getPeakUserMemoryBytes();
        peakTotalNonRevocableMemoryBytes = statistics.getPeakTotalNonRevocableMemoryBytes();
        peakTaskUserMemory = statistics.getPeakTaskUserMemory();
        peakTaskTotalMemory = statistics.getPeakTaskTotalMemory();
        physicalInputBytes = statistics.getPhysicalInputBytes();
        physicalInputRows = statistics.getPhysicalInputRows();
        internalNetworkBytes = statistics.getInternalNetworkBytes();
        internalNetworkRows = statistics.getInternalNetworkRows();
        totalBytes = statistics.getTotalBytes();
        totalRows = statistics.getTotalRows();
        outputBytes = statistics.getOutputBytes();
        outputRows = statistics.getOutputRows();
        writtenBytes = statistics.getWrittenBytes();
        writtenRows = statistics.getWrittenRows();
        cumulativeMemory = statistics.getCumulativeMemory();
        complete = statistics.isComplete();
        completedSplits = statistics.getCompletedSplits();

        QueryContext queryContext = queryCompletedEvent.getContext();
        user = queryContext.getUser();
        principal = getFromOptionalString(queryContext.getPrincipal());
        remoteClientAddress = getFromOptionalString(queryContext.getRemoteClientAddress());
        userAgent = getFromOptionalString(queryContext.getUserAgent());
        clientInfo = getFromOptionalString(queryContext.getClientInfo());
        source = getFromOptionalString(queryContext.getSource());
        catalog = getFromOptionalString(queryContext.getCatalog());
        schema = getFromOptionalString(queryContext.getSchema());
        serverAddress = queryContext.getServerAddress();
        serverVersion = queryContext.getServerVersion();
        environment = queryContext.getEnvironment();

        String enableHiveSyntax = queryContext.getSessionProperties().get("enable_hive_syntax");
        syntax = enableHiveSyntax == null? "presto":enableHiveSyntax.equals("true")?"hive":"presto";
        sessionProperties = JSON.toJSONString(queryContext.getSessionProperties());

        if (queryCompletedEvent.getFailureInfo().isPresent()) {
            QueryFailureInfo queryFailureInfo = queryCompletedEvent.getFailureInfo().get();
            failureType = getFromOptionalString(queryFailureInfo.getFailureType());
            failureMessage = getFromOptionalString(queryFailureInfo.getFailureMessage());
            failureHost = getFromOptionalString(queryFailureInfo.getFailureHost());
            failuresJson = queryFailureInfo.getFailuresJson();
        }

        createTime = queryCompletedEvent.getCreateTime().atOffset(ZoneOffset.ofHours(8)).toString();
        executionStartTime = queryCompletedEvent.getExecutionStartTime().atOffset(ZoneOffset.ofHours(8)).toString();
        endTime = queryCompletedEvent.getEndTime().atOffset(ZoneOffset.ofHours(8)).toString();

        prestoWarnings = queryCompletedEvent.getWarnings();

        inputs = queryCompletedEvent.getIoMetadata().getInputs();
        if (queryCompletedEvent.getIoMetadata().getOutput().isPresent()) {
            QueryOutputMetadata queryOutputMetadata = queryCompletedEvent.getIoMetadata().getOutput().get();
            catalogName = queryOutputMetadata.getCatalogName();
            outputSchema = queryOutputMetadata.getSchema();
            table = queryOutputMetadata.getTable();
        }
    }

    public static Duration getFromOptionalDuration(Optional<Duration> data)
    {
        return data.isPresent() ? data.get() : null;
    }

    public static String getFromOptionalString(Optional<String> data)
    {
        return data.isPresent() ? data.get() : null;
    }

    @Override
    public String toString() {

        JSONObject jsonObject = new JSONObject();
        Field[] fields = this.getClass().getDeclaredFields();
        for (Field field: fields) {
            String name = CaseFormat.LOWER_CAMEL.to(CaseFormat.LOWER_UNDERSCORE, field.getName());
            field.setAccessible(true);
            Object value;
            try {
                value = field.get(this);

                if (name.equals("presto_warnings")) {
                    // prestoWarnings
                    List<PrestoWarning> prestoWarningsArray = (List<PrestoWarning>) value;
                    JSONArray prestoWarningsJson = new JSONArray();
                    for (PrestoWarning prestoWarning: prestoWarningsArray) {
                        JSONObject warningJson = new JSONObject();
                        warningJson.put("warning_name", prestoWarning.getWarningCode().getName());
                        warningJson.put("warning_code", prestoWarning.getWarningCode().getCode());
                        warningJson.put("warning_message", prestoWarning.getMessage());
                        prestoWarningsJson.add(warningJson);
                    }
                    jsonObject.put(name, prestoWarningsJson);
                    continue;
                } else if (name.equals("inputs")) {
                    // inputs
                    JSONArray inputsJson = new JSONArray();
                    List<QueryInputMetadata> inputs = (List<QueryInputMetadata>) value;
                    for (QueryInputMetadata input: inputs) {
                        JSONObject inputJson = new JSONObject();
                        inputJson.put("catalog_name", input.getCatalogName());
                        inputJson.put("schema", input.getSchema());
                        inputJson.put("table", input.getTable());
                        JSONArray columnsJson = new JSONArray();
                        List<String> columns = input.getColumns();
                        for (String column : columns) {
                            columnsJson.add(column);
                        }
                        inputJson.put("columns", columnsJson);
                        inputsJson.add(inputJson);
                    }
                    jsonObject.put("inputs", inputsJson);
                    continue;
                } else if (name.equals("log")) {
                    // ignore log field
                    continue;
                }
                jsonObject.put(name, value);
            } catch (IllegalAccessException e) {
                log.error("", e);
            }
        }
        return jsonObject.toString();
    }
}
