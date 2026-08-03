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
package io.trino.plugin.doris;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.airlift.http.client.HeaderName;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.Request;
import io.airlift.http.client.StringResponseHandler.StringResponse;
import io.airlift.http.client.testing.TestingHttpClient;
import io.airlift.json.JsonCodec;
import io.airlift.json.JsonCodecFactory;
import io.trino.spi.TrinoException;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.airlift.http.client.HttpStatus.OK;
import static io.airlift.http.client.Request.Builder.preparePost;
import static io.airlift.http.client.StaticBodyGenerator.createStaticBodyGenerator;
import static io.airlift.http.client.StringResponseHandler.createStringResponseHandler;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.util.Objects.requireNonNull;

public class FeDorisSplitPlanner
        implements DorisSplitPlanner
{
    private static final JsonCodec<QueryPlanEnvelope> QUERY_PLAN_ENVELOPE_CODEC = new JsonCodecFactory().jsonCodec(QueryPlanEnvelope.class);
    private static final JsonCodec<QueryPlanRequest> QUERY_PLAN_REQUEST_CODEC = new JsonCodecFactory().jsonCodec(QueryPlanRequest.class);

    private final DorisConfig config;
    private final DorisQueryBuilder queryBuilder;
    private final HttpClient httpClient;
    private final String basicAuthHeader;

    private volatile List<DorisFeEndpoints.FeEndpoint> cachedFeEndpoints;
    private volatile DorisFeEndpoints.FeEndpoint preferredFeEndpoint;

    @Inject
    public FeDorisSplitPlanner(DorisConfig config, DorisQueryBuilder queryBuilder, @ForDoris HttpClient httpClient)
    {
        this.config = requireNonNull(config, "config is null");
        this.queryBuilder = requireNonNull(queryBuilder, "queryBuilder is null");
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
        this.basicAuthHeader = buildBasicAuthHeader(config);
    }

    FeDorisSplitPlanner(DorisConfig config, DorisQueryBuilder queryBuilder)
    {
        this(config, queryBuilder, new TestingHttpClient(_ -> {
            throw new UnsupportedOperationException("HTTP client should not be used in this test path");
        }));
    }

    @Override
    public List<DorisSplit> planSplits(DorisTableHandle tableHandle)
    {
        String sql = queryBuilder.buildSplitPlanningSql(tableHandle);
        DorisQueryPlanResponse queryPlan = fetchQueryPlan(tableHandle, sql);
        List<DorisSplit> splits = buildSplits(tableHandle, queryPlan);
        return consolidateSplits(tableHandle, splits);
    }

    private List<DorisSplit> consolidateSplits(DorisTableHandle tableHandle, List<DorisSplit> splits)
    {
        int maxSplits = config.getMaxSplitsPerQuery();
        int minTabletsPerSplit = config.getMinTabletsPerSplit();

        // If already within limits, return as-is
        if (splits.size() <= maxSplits) {
            return splits;
        }

        // Calculate total tablets
        int totalTablets = splits.stream()
                .mapToInt(split -> split.tabletIds().size())
                .sum();

        // Calculate target tablets per split to stay under maxSplits
        int targetTabletsPerSplit = Math.max(minTabletsPerSplit, (totalTablets + maxSplits - 1) / maxSplits);

        // Consolidate splits by merging adjacent ones
        List<DorisSplit> consolidated = new ArrayList<>();
        List<Long> currentTablets = new ArrayList<>();
        String currentBackend = null;
        Optional<String> currentQueryPlan = Optional.empty();

        for (DorisSplit split : splits) {
            if (currentBackend == null) {
                currentBackend = split.beAddress();
                currentTablets.addAll(split.tabletIds());
                currentQueryPlan = split.opaquedQueryPlan();
            }
            else if (currentBackend.equals(split.beAddress()) && currentQueryPlan.equals(split.opaquedQueryPlan()) && currentTablets.size() < targetTabletsPerSplit) {
                // Merge with current split if same backend and under target
                currentTablets.addAll(split.tabletIds());
            }
            else {
                // Flush current split
                consolidated.add(new DorisSplit(
                        tableHandle.remoteSchemaName(),
                        tableHandle.remoteTableName(),
                        currentBackend,
                        List.copyOf(currentTablets),
                        currentQueryPlan));
                currentBackend = split.beAddress();
                currentTablets = new ArrayList<>(split.tabletIds());
                currentQueryPlan = split.opaquedQueryPlan();
            }
        }

        // Flush last split
        if (currentBackend != null && !currentTablets.isEmpty()) {
            consolidated.add(new DorisSplit(
                    tableHandle.remoteSchemaName(),
                    tableHandle.remoteTableName(),
                    currentBackend,
                    List.copyOf(currentTablets),
                    currentQueryPlan));
        }

        return consolidated;
    }

    static List<DorisSplit> buildSplits(DorisTableHandle tableHandle, DorisQueryPlanResponse queryPlan)
    {
        Map<String, List<Long>> beToTablets = selectBackends(queryPlan);

        List<Map.Entry<String, List<Long>>> entries = new ArrayList<>(beToTablets.entrySet());
        entries.sort(Map.Entry.comparingByKey());

        List<DorisSplit> splits = new ArrayList<>(entries.size());
        for (Map.Entry<String, List<Long>> entry : entries) {
            List<Long> tabletIds = entry.getValue().stream()
                    .sorted()
                    .toList();
            splits.add(new DorisSplit(
                    tableHandle.remoteSchemaName(),
                    tableHandle.remoteTableName(),
                    entry.getKey(),
                    tabletIds,
                    Optional.ofNullable(queryPlan.opaquedQueryPlan())));
        }
        return List.copyOf(splits);
    }

    static Map<String, List<Long>> selectBackends(DorisQueryPlanResponse queryPlan)
    {
        Map<String, List<Long>> beToTablets = new LinkedHashMap<>();
        List<Map.Entry<String, DorisQueryPlanTablet>> tablets = queryPlan.partitions().entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .toList();

        for (Map.Entry<String, DorisQueryPlanTablet> partition : tablets) {
            long tabletId = parseTabletId(partition.getKey());
            String targetBackend = chooseBackend(beToTablets, partition.getValue().routings(), tabletId);
            beToTablets.computeIfAbsent(targetBackend, _ -> new ArrayList<>())
                    .add(tabletId);
        }
        return beToTablets;
    }

    private DorisQueryPlanResponse fetchQueryPlan(DorisTableHandle tableHandle, String sql)
    {
        List<String> failures = new ArrayList<>();
        for (DorisFeEndpoints.FeEndpoint feEndpoint : prioritizedFeEndpoints()) {
            for (URI uri : feEndpoint.queryPlanUris(tableHandle.remoteSchemaName(), tableHandle.remoteTableName())) {
                Request request = preparePost()
                        .setUri(uri)
                        .addHeader(HeaderName.of("Authorization"), basicAuthHeader)
                        .addHeader(HeaderName.of("Content-Type"), "application/json; charset=UTF-8")
                        .setBodyGenerator(createStaticBodyGenerator(queryPlanRequestBody(sql), StandardCharsets.UTF_8))
                        .build();

                try {
                    StringResponse response = httpClient.execute(request, createStringResponseHandler());
                    if (response.getStatusCode() != OK.code()) {
                        failures.add("%s -> HTTP %s".formatted(uri, response.getStatusCode()));
                        continue;
                    }
                    preferredFeEndpoint = feEndpoint;
                    return parseQueryPlan(response.getBody());
                }
                catch (RuntimeException e) {
                    failures.add("%s -> %s".formatted(uri, e.getMessage()));
                }
            }
        }

        throw new TrinoException(GENERIC_INTERNAL_ERROR, "Failed to fetch Doris query plan from FE nodes: " + failures);
    }

    private List<DorisFeEndpoints.FeEndpoint> prioritizedFeEndpoints()
    {
        List<DorisFeEndpoints.FeEndpoint> endpoints = cachedFeEndpoints;
        if (endpoints == null) {
            endpoints = DorisFeEndpoints.getEndpoints(config);
            cachedFeEndpoints = endpoints;
        }

        DorisFeEndpoints.FeEndpoint preferred = preferredFeEndpoint;
        if (preferred == null || !endpoints.contains(preferred)) {
            return endpoints;
        }

        ImmutableList.Builder<DorisFeEndpoints.FeEndpoint> prioritized = ImmutableList.builderWithExpectedSize(endpoints.size());
        prioritized.add(preferred);
        for (DorisFeEndpoints.FeEndpoint endpoint : endpoints) {
            if (!endpoint.equals(preferred)) {
                prioritized.add(endpoint);
            }
        }
        return prioritized.build();
    }

    private DorisQueryPlanResponse parseQueryPlan(String responseBody)
    {
        QueryPlanEnvelope envelope;
        try {
            envelope = QUERY_PLAN_ENVELOPE_CODEC.fromJson(responseBody);
        }
        catch (IllegalArgumentException e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Invalid JSON string for Doris query plan response: " + responseBody, e);
        }

        DorisQueryPlanResponse queryPlan = envelope.data();
        if (queryPlan == null) {
            String message = envelope.msg().filter(value -> !value.isBlank())
                    .orElse("no data in response");
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Doris FE query plan response does not contain query plan data: " + message);
        }
        if (queryPlan.status() != 200) {
            String message = envelope.msg().orElse("");
            throw new TrinoException(
                    GENERIC_INTERNAL_ERROR,
                    "Doris FE query plan status is not OK: " + queryPlan.status() + (message.isBlank() ? "" : " (" + message + ")"));
        }
        return queryPlan;
    }

    private String queryPlanRequestBody(String sql)
    {
        return QUERY_PLAN_REQUEST_CODEC.toJson(new QueryPlanRequest(sql));
    }

    private static String buildBasicAuthHeader(DorisConfig config)
    {
        String user = config.getUsername().orElse("");
        String password = config.getPassword().orElse("");
        String credentials = user + ":" + password;
        return "Basic " + Base64.getEncoder().encodeToString(credentials.getBytes(StandardCharsets.UTF_8));
    }

    private static String chooseBackend(Map<String, List<Long>> beToTablets, List<String> routings, long tabletId)
    {
        String targetBackend = null;
        int smallestAssignment = Integer.MAX_VALUE;

        for (String routing : routings) {
            if (!beToTablets.containsKey(routing)) {
                return routing;
            }

            int assignmentCount = beToTablets.get(routing).size();
            if (assignmentCount < smallestAssignment) {
                targetBackend = routing;
                smallestAssignment = assignmentCount;
            }
        }

        if (targetBackend == null) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "No Doris backend is available for tablet " + tabletId);
        }
        return targetBackend;
    }

    private static long parseTabletId(String value)
    {
        try {
            return Long.parseLong(value);
        }
        catch (NumberFormatException e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Invalid Doris tablet id: " + value, e);
        }
    }

    public record QueryPlanRequest(String sql) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record QueryPlanEnvelope(
            @JsonProperty("status") Optional<Integer> code,
            @JsonProperty("msg") Optional<String> msg,
            @JsonProperty("data") DorisQueryPlanResponse data) {}
}
