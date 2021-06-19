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
package io.trino.plugin.pinot.client;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.net.HttpHeaders;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.JsonResponseHandler;
import io.airlift.http.client.Request;
import io.airlift.http.client.StaticBodyGenerator;
import io.airlift.http.client.UnexpectedResponseException;
import io.airlift.json.JsonCodec;
import io.airlift.json.JsonCodecBinder;
import io.airlift.json.JsonCodecFactory;
import io.airlift.log.Logger;
import io.trino.plugin.pinot.ForPinot;
import io.trino.plugin.pinot.PinotColumnHandle;
import io.trino.plugin.pinot.PinotConfig;
import io.trino.plugin.pinot.PinotErrorCode;
import io.trino.plugin.pinot.PinotException;
import io.trino.plugin.pinot.PinotInsufficientServerResponseException;
import io.trino.plugin.pinot.PinotSessionProperties;
import io.trino.plugin.pinot.query.PinotQuery;
import io.trino.spi.connector.ConnectorSession;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.common.response.broker.ResultTable;
import org.apache.pinot.spi.data.Schema;

import javax.inject.Inject;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.airlift.http.client.JsonResponseHandler.createJsonResponseHandler;
import static io.airlift.json.JsonCodec.listJsonCodec;
import static io.airlift.json.JsonCodec.mapJsonCodec;
import static io.trino.plugin.pinot.PinotErrorCode.PINOT_EXCEPTION;
import static io.trino.plugin.pinot.PinotErrorCode.PINOT_INVALID_CONFIGURATION;
import static io.trino.plugin.pinot.PinotErrorCode.PINOT_UNABLE_TO_FIND_BROKER;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.function.UnaryOperator.identity;
import static org.apache.pinot.spi.utils.builder.TableNameBuilder.extractRawTableName;

public class PinotClient
{
    private static final Logger LOG = Logger.get(PinotClient.class);
    private static final String APPLICATION_JSON = "application/json";
    private static final Pattern BROKER_PATTERN = Pattern.compile("Broker_(.*)_(\\d+)");
    private static final String TIME_BOUNDARY_NOT_FOUND_ERROR_CODE = "404";
    private static final JsonCodec<Map<String, Map<String, List<String>>>> ROUTING_TABLE_CODEC = mapJsonCodec(String.class, mapJsonCodec(String.class, listJsonCodec(String.class)));

    private static final String GET_ALL_TABLES_API_TEMPLATE = "tables";
    private static final String TABLE_INSTANCES_API_TEMPLATE = "tables/%s/instances";
    private static final String TABLE_SCHEMA_API_TEMPLATE = "tables/%s/schema";
    private static final String ROUTING_TABLE_API_TEMPLATE = "debug/routingTable/%s";
    private static final String TIME_BOUNDARY_API_TEMPLATE = "debug/timeBoundary/%s";
    private static final String REQUEST_PAYLOAD_TEMPLATE = "{\"sql\" : \"%s\" }";
    private static final String QUERY_URL_TEMPLATE = "http://%s/query/sql";

    private final List<String> controllerUrls;
    private final HttpClient httpClient;
    private final PinotHostMapper pinotHostMapper;

    private final LoadingCache<String, List<String>> brokersForTableCache;

    private final JsonCodec<GetTables> tablesJsonCodec;
    private final JsonCodec<BrokersForTable> brokersForTableJsonCodec;
    private final JsonCodec<TimeBoundary> timeBoundaryJsonCodec;
    private final JsonCodec<Schema> schemaJsonCodec;
    private final JsonCodec<BrokerResponseNative> brokerResponseCodec;

    @Inject
    public PinotClient(
            PinotConfig config,
            PinotHostMapper pinotHostMapper,
            @ForPinot HttpClient httpClient,
            JsonCodec<GetTables> tablesJsonCodec,
            JsonCodec<BrokersForTable> brokersForTableJsonCodec,
            JsonCodec<TimeBoundary> timeBoundaryJsonCodec,
            JsonCodec<BrokerResponseNative> brokerResponseCodec)
    {
        this.brokersForTableJsonCodec = requireNonNull(brokersForTableJsonCodec, "brokersForTableJsonCodec is null");
        this.timeBoundaryJsonCodec = requireNonNull(timeBoundaryJsonCodec, "timeBoundaryJsonCodec is null");
        this.tablesJsonCodec = requireNonNull(tablesJsonCodec, "tablesJsonCodec is null");
        this.schemaJsonCodec = new JsonCodecFactory(() -> new ObjectMapper()
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)).jsonCodec(Schema.class);
        this.brokerResponseCodec = requireNonNull(brokerResponseCodec, "brokerResponseCodec is null");
        requireNonNull(config, "config is null");
        if (config.getControllerUrls() == null || config.getControllerUrls().isEmpty()) {
            throw new PinotException(PINOT_INVALID_CONFIGURATION, Optional.empty(), "No pinot controllers specified");
        }
        this.pinotHostMapper = requireNonNull(pinotHostMapper, "pinotHostMapper is null");

        this.controllerUrls = config.getControllerUrls();
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
        this.brokersForTableCache = CacheBuilder.newBuilder()
                .expireAfterWrite(config.getMetadataCacheExpiry().roundTo(TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS)
                .build((CacheLoader.from(this::getAllBrokersForTable)));
    }

    public static JsonCodecBinder addJsonBinders(JsonCodecBinder jsonCodecBinder)
    {
        jsonCodecBinder.bindJsonCodec(GetTables.class);
        jsonCodecBinder.bindJsonCodec(BrokersForTable.InstancesInBroker.class);
        jsonCodecBinder.bindJsonCodec(BrokersForTable.class);
        jsonCodecBinder.bindJsonCodec(TimeBoundary.class);
        jsonCodecBinder.bindJsonCodec(BrokerResponseNative.class);
        return jsonCodecBinder;
    }

    protected <T> T doHttpActionWithHeadersJson(Request.Builder requestBuilder, Optional<String> requestBody, JsonCodec<T> codec)
    {
        requestBuilder.setHeader(HttpHeaders.ACCEPT, APPLICATION_JSON);
        if (requestBody.isPresent()) {
            requestBuilder.setHeader(HttpHeaders.CONTENT_TYPE, APPLICATION_JSON);
            requestBuilder.setBodyGenerator(StaticBodyGenerator.createStaticBodyGenerator(requestBody.get(), StandardCharsets.UTF_8));
        }
        Request request = requestBuilder.build();
        JsonResponseHandler<T> responseHandler = createJsonResponseHandler(codec);
        T response = null;
        try {
            response = httpClient.execute(request, responseHandler);
        }
        catch (UnexpectedResponseException e) {
            throw new PinotException(
                    PinotErrorCode.PINOT_HTTP_ERROR,
                    Optional.empty(),
                    format(
                            "Unexpected response status: %d for request %s to url %s, with headers %s, full response %s",
                            e.getStatusCode(),
                            requestBody.orElse(""),
                            request.getUri(),
                            request.getHeaders(),
                            response));
        }
        return response;
    }

    private <T> T sendHttpGetToControllerJson(String path, JsonCodec<T> codec)
    {
        return doHttpActionWithHeadersJson(
                Request.Builder.prepareGet().setUri(URI.create(format("http://%s/%s", getControllerUrl(), path))),
                Optional.empty(),
                codec);
    }

    private <T> T sendHttpGetToBrokerJson(String table, String path, JsonCodec<T> codec)
    {
        return doHttpActionWithHeadersJson(
                Request.Builder.prepareGet().setUri(URI.create(format("http://%s/%s", getBrokerHost(table), path))),
                Optional.empty(),
                codec);
    }

    private String getControllerUrl()
    {
        return controllerUrls.get(ThreadLocalRandom.current().nextInt(controllerUrls.size()));
    }

    public static class GetTables
    {
        private final List<String> tables;

        @JsonCreator
        public GetTables(@JsonProperty("tables") List<String> tables)
        {
            this.tables = tables;
        }

        public List<String> getTables()
        {
            return tables;
        }
    }

    public List<String> getAllTables()
    {
        return sendHttpGetToControllerJson(GET_ALL_TABLES_API_TEMPLATE, tablesJsonCodec).getTables();
    }

    public Schema getTableSchema(String table)
            throws Exception
    {
        return sendHttpGetToControllerJson(format(TABLE_SCHEMA_API_TEMPLATE, table), schemaJsonCodec);
    }

    public static class BrokersForTable
    {
        public static class InstancesInBroker
        {
            private final List<String> instances;

            @JsonCreator
            public InstancesInBroker(@JsonProperty("instances") List<String> instances)
            {
                this.instances = instances;
            }

            @JsonProperty("instances")
            public List<String> getInstances()
            {
                return instances;
            }
        }

        private final List<InstancesInBroker> brokers;

        @JsonCreator
        public BrokersForTable(@JsonProperty("brokers") List<InstancesInBroker> brokers)
        {
            this.brokers = brokers;
        }

        @JsonProperty("brokers")
        public List<InstancesInBroker> getBrokers()
        {
            return brokers;
        }
    }

    @VisibleForTesting
    public List<String> getAllBrokersForTable(String table)
    {
        ArrayList<String> brokers = sendHttpGetToControllerJson(format(TABLE_INSTANCES_API_TEMPLATE, table), brokersForTableJsonCodec)
                .getBrokers().stream()
                .flatMap(broker -> broker.getInstances().stream())
                .distinct()
                .map(brokerToParse -> {
                    Matcher matcher = BROKER_PATTERN.matcher(brokerToParse);
                    if (matcher.matches() && matcher.groupCount() == 2) {
                        return pinotHostMapper.getBrokerHost(matcher.group(1), matcher.group(2));
                    }
                    else {
                        throw new PinotException(
                                PINOT_UNABLE_TO_FIND_BROKER,
                                Optional.empty(),
                                format("Cannot parse %s in the broker instance", brokerToParse));
                    }
                })
                .collect(Collectors.toCollection(ArrayList::new));
        Collections.shuffle(brokers);
        return ImmutableList.copyOf(brokers);
    }

    public String getBrokerHost(String table)
    {
        try {
            List<String> brokers = brokersForTableCache.get(table);
            if (brokers.isEmpty()) {
                throw new PinotException(PINOT_UNABLE_TO_FIND_BROKER, Optional.empty(), "No valid brokers found for " + table);
            }
            return brokers.get(ThreadLocalRandom.current().nextInt(brokers.size()));
        }
        catch (ExecutionException e) {
            Throwable throwable = e.getCause();
            if (throwable instanceof PinotException) {
                throw (PinotException) throwable;
            }
            else {
                throw new PinotException(PINOT_UNABLE_TO_FIND_BROKER, Optional.empty(), "Error when getting brokers for table " + table, throwable);
            }
        }
    }

    public Map<String, Map<String, List<String>>> getRoutingTableForTable(String tableName)
    {
        Map<String, Map<String, List<String>>> routingTable = sendHttpGetToBrokerJson(tableName, format(ROUTING_TABLE_API_TEMPLATE, tableName), ROUTING_TABLE_CODEC);
        ImmutableMap.Builder<String, Map<String, List<String>>> routingTableMap = ImmutableMap.builder();
        for (Map.Entry<String, Map<String, List<String>>> entry : routingTable.entrySet()) {
            String tablenameWithType = entry.getKey();
            if (!entry.getValue().isEmpty() && tableName.equals(extractRawTableName(tablenameWithType))) {
                ImmutableMap.Builder<String, List<String>> segmentBuilder = ImmutableMap.builder();
                for (Map.Entry<String, List<String>> segmentEntry : entry.getValue().entrySet()) {
                    if (!segmentEntry.getValue().isEmpty()) {
                        segmentBuilder.put(segmentEntry.getKey(), segmentEntry.getValue());
                    }
                }
                Map<String, List<String>> segmentMap = segmentBuilder.build();
                if (!segmentMap.isEmpty()) {
                    routingTableMap.put(tablenameWithType, segmentMap);
                }
            }
        }
        return routingTableMap.build();
    }

    public static class TimeBoundary
    {
        private final Optional<String> onlineTimePredicate;
        private final Optional<String> offlineTimePredicate;

        public TimeBoundary()
        {
            this(null, null);
        }

        @JsonCreator
        public TimeBoundary(
                @JsonProperty String timeColumnName,
                @JsonProperty String timeColumnValue)
        {
            if (timeColumnName != null && timeColumnValue != null) {
                offlineTimePredicate = Optional.of(format("%s < %s", timeColumnName, timeColumnValue));
                onlineTimePredicate = Optional.of(format("%s >= %s", timeColumnName, timeColumnValue));
            }
            else {
                onlineTimePredicate = Optional.empty();
                offlineTimePredicate = Optional.empty();
            }
        }

        public Optional<String> getOnlineTimePredicate()
        {
            return onlineTimePredicate;
        }

        public Optional<String> getOfflineTimePredicate()
        {
            return offlineTimePredicate;
        }
    }

    public TimeBoundary getTimeBoundaryForTable(String table)
    {
        try {
            return sendHttpGetToBrokerJson(table, format(TIME_BOUNDARY_API_TEMPLATE, table), timeBoundaryJsonCodec);
        }
        catch (Exception e) {
            String[] errorMessageSplits = e.getMessage().split(" ");
            if (errorMessageSplits.length >= 4 && errorMessageSplits[3].equalsIgnoreCase(TIME_BOUNDARY_NOT_FOUND_ERROR_CODE)) {
                return timeBoundaryJsonCodec.fromJson("{}");
            }
            throw e;
        }
    }

    public interface BrokerResultRow
    {
        Object getField(int index);
    }

    private static class ResultRow
            implements BrokerResultRow
    {
        private final Object[] row;
        private final int[] indices;

        public ResultRow(Object[] row, int[] indices)
        {
            this.row = requireNonNull(row, "row is null");
            this.indices = requireNonNull(indices, "indices is null");
        }

        @Override
        public Object getField(int index)
        {
            return row[indices[index]];
        }
    }

    public static class ResultsIterator
            extends AbstractIterator<BrokerResultRow>
    {
        private final List<Object[]> rows;
        private final int[] indices;
        private int rowIndex;

        public ResultsIterator(ResultTable resultTable, int[] indices)
        {
            requireNonNull(resultTable, "resultTable is null");
            this.rows = resultTable.getRows();
            this.indices = requireNonNull(indices, "indices is null");
        }

        @Override
        protected BrokerResultRow computeNext()
        {
            if (rowIndex == rows.size()) {
                return endOfData();
            }
            return new ResultRow(rows.get(rowIndex++), indices);
        }
    }

    private BrokerResponseNative submitBrokerQueryJson(ConnectorSession session, PinotQuery query)
    {
        return doWithRetries(PinotSessionProperties.getPinotRetryCount(session), retryNumber -> {
            String queryHost = getBrokerHost(query.getTable());
            LOG.info("Query '%s' on broker host '%s'", queryHost, query.getQuery());
            Request.Builder builder = Request.Builder.preparePost()
                    .setUri(URI.create(format(QUERY_URL_TEMPLATE, queryHost)));
            BrokerResponseNative response = doHttpActionWithHeadersJson(builder, Optional.of(format(REQUEST_PAYLOAD_TEMPLATE, query.getQuery())), brokerResponseCodec);

            if (response.getExceptionsSize() > 0 && response.getProcessingExceptions() != null && !response.getProcessingExceptions().isEmpty()) {
                // Pinot is known to return exceptions with benign errorcodes like 200
                // so we treat any exception as an error
                throw new PinotException(
                        PINOT_EXCEPTION,
                        Optional.of(query.getQuery()),
                        format("Query %s encountered exception %s", query.getQuery(), response.getProcessingExceptions().get(0)));
            }
            if (response.getNumServersQueried() == 0 || response.getNumServersResponded() == 0 || response.getNumServersQueried() > response.getNumServersResponded()) {
                throw new PinotInsufficientServerResponseException(query, response.getNumServersResponded(), response.getNumServersQueried());
            }

            return response;
        });
    }

    /**
     * columnIndices: column name -> column index from column handles
     * indiceToGroupByFunction<Int,String> (groupByFunctions): aggregationIndex -> groupByFunctionName(columnName)
     * groupByFunctions is for values
     * groupByColumnNames: from aggregationResult.groupByResult.groupByColumnNames()
     * aggregationResults[GroupByColumns, GroupByResult]
     * GroupByColumns:  String[] // column names, i.e. group by foo, bar, baz
     * GroupByResult: Row[]
     * Row: {group: String[] // values of groupBy columns, value: aggregationResult}
     * <p>
     * Results: aggregationResults.get(0..aggregationResults.size())
     * Result: function, value means columnName -> columnValue
     */
    public Iterator<BrokerResultRow> createResultIterator(ConnectorSession session, PinotQuery query, List<PinotColumnHandle> columnHandles)
    {
        BrokerResponseNative response = submitBrokerQueryJson(session, query);
        return fromResultTable(response.getResultTable(), columnHandles);
    }

    @VisibleForTesting
    public static ResultsIterator fromResultTable(ResultTable resultTable, List<PinotColumnHandle> columnHandles)
    {
        requireNonNull(resultTable, "resultTable is null");
        requireNonNull(columnHandles, "columnHandles is null");
        String[] columnNames = resultTable.getDataSchema().getColumnNames();
        Map<String, Integer> columnIndices = IntStream.range(0, columnNames.length)
                .boxed()
                // Pinot lower cases column names which use aggregate functions, ex. min(my_Col) becomes min(my_col)
                .collect(toImmutableMap(i -> columnNames[i].toLowerCase(ENGLISH), identity()));
        int[] indices = new int[columnNames.length];
        for (int i = 0; i < columnHandles.size(); i++) {
            indices[i] = columnIndices.get(columnHandles.get(i).getColumnName().toLowerCase(ENGLISH));
        }
        return new ResultsIterator(resultTable, indices);
    }

    public static <T> T doWithRetries(int retries, Function<Integer, T> caller)
    {
        PinotException firstError = null;
        checkState(retries > 0, "Invalid num of retries %s", retries);
        for (int i = 0; i < retries; ++i) {
            try {
                return caller.apply(i);
            }
            catch (PinotException e) {
                if (firstError == null) {
                    firstError = e;
                }
                if (!e.isRetriable()) {
                    throw e;
                }
            }
        }
        throw firstError;
    }
}
