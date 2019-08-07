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
package io.prestosql.plugin.prometheus;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Files;
import io.airlift.json.JsonCodec;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.type.DoubleType;
import io.prestosql.spi.type.MapType;
import io.prestosql.spi.type.TimestampType;
import io.prestosql.spi.type.TypeManager;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.ResponseBody;
import org.apache.http.client.utils.URIBuilder;

import javax.inject.Inject;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static io.prestosql.plugin.prometheus.PrometheusErrorCode.PROMETHEUS_TABLES_METRICS_RETRIEVE_ERROR;
import static io.prestosql.plugin.prometheus.PrometheusErrorCode.PROMETHEUS_UNKNOWN_ERROR;
import static io.prestosql.spi.StandardErrorCode.NOT_FOUND;
import static io.prestosql.spi.type.TypeSignature.mapType;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

public class PrometheusClient
{
    private PrometheusConnectorConfig config;
    static final String METRICS_ENDPOINT = "/api/v1/label/__name__/values";
    private static final OkHttpClient httpClient = new OkHttpClient.Builder().build();
    private final Supplier<Map<String, Object>> tableSupplier;
    private final TypeManager typeManager;

    @Inject
    public PrometheusClient(PrometheusConnectorConfig config, JsonCodec<Map<String, Object>> metricCodec, TypeManager typeManager)
            throws URISyntaxException
    {
        requireNonNull(config, "config is null");
        requireNonNull(metricCodec, "metricCodec is null");
        requireNonNull(typeManager, "typeManager is null");

        tableSupplier = Suppliers.memoizeWithExpiration(metricsSupplier(metricCodec, getPrometheusMetricsURI(config)),
                (long) config.getCacheDuration().getValue(), config.getCacheDuration().getUnit());
        this.typeManager = typeManager;
        this.config = config;
    }

    private URI getPrometheusMetricsURI(PrometheusConnectorConfig config)
            throws URISyntaxException
    {
        // endpoint to retrieve metric names from Prometheus
        URI uri = config.getPrometheusURI();
        return new URIBuilder()
                .setScheme(uri.getScheme())
                .setHost(uri.getAuthority())
                .setPath(uri.getPath().concat(METRICS_ENDPOINT))
                .build();
    }

    public Set<String> getTableNames(String schema)
    {
        requireNonNull(schema, "schema is null");
        String status = "";
        if (schema.equals("default")) {
            status = (String) tableSupplier.get().get("status");
            //TODO prometheus warnings (success|error|warning) could be handled separately
            if (status.equals("success")) {
                List<String> tableNames = (List<String>) tableSupplier.get().get("data");
                if (tableNames == null) {
                    return ImmutableSet.of();
                }
                return ImmutableSet.copyOf(tableNames);
            }
        }
        throw new PrestoException(PROMETHEUS_TABLES_METRICS_RETRIEVE_ERROR, String.format("Prometheus did no return metrics list (table names): %s", status));
    }

    public PrometheusTable getTable(String schema, String tableName)
    {
        requireNonNull(schema, "schema is null");
        requireNonNull(tableName, "tableName is null");
        if (schema.equals("default")) {
            List<String> tableNames = (List<String>) tableSupplier.get().get("data");
            if (tableNames == null) {
                return null;
            }
            if (!tableNames.contains(tableName)) {
                return null;
            }
            MapType varcharMapType = (MapType) typeManager.getType(mapType(VARCHAR.getTypeSignature(), VARCHAR.getTypeSignature()));
            PrometheusTable table = new PrometheusTable(
                    tableName,
                    ImmutableList.of(
                            new PrometheusColumn("labels", varcharMapType),
                            new PrometheusColumn("timestamp", TimestampType.TIMESTAMP),
                            new PrometheusColumn("value", DoubleType.DOUBLE)));
            PrometheusTableHandle tableHandle = new PrometheusTableHandle(schema, tableName);
            return table;
        }
        return null;
    }

    private static Supplier<Map<String, Object>> metricsSupplier(final JsonCodec<Map<String, Object>> metricsCodec, final URI metadataUri)
    {
        return () -> {
            try {
                byte[] json = getHttpResponse(metadataUri).bytes();
                Map<String, Object> metrics = metricsCodec.fromJson(json);
                return metrics;
            }
            catch (IOException | URISyntaxException e) {
                throw new UncheckedIOException((IOException) e);
            }
        };
    }

    static ResponseBody getHttpResponse(URI uri)
            throws IOException, URISyntaxException
    {
        Request.Builder requestBuilder = new Request.Builder();
        if (new PrometheusConnectorConfig() != null) {
            getBearerAuthInfoFromFile().map(bearerToken ->
                    requestBuilder.header("Authorization", "Bearer " + bearerToken));
        }
        requestBuilder.url(uri.toURL());
        Request request = requestBuilder.build();
        Response response = httpClient.newCall(request).execute();
        if (response.isSuccessful()) {
            return response.body();
        }
        else {
            throw new PrestoException(PROMETHEUS_UNKNOWN_ERROR, "Bad response " + response.code() + response.message());
        }
    }

    static Optional<String> getBearerAuthInfoFromFile()
            throws URISyntaxException
    {
        return new PrometheusConnectorConfig().getBearerTokenFile()
                .map(tokenFileName -> {
                    try {
                        File tokenFile = tokenFileName;
                        return Optional.of(Files.toString(tokenFile, UTF_8));
                    }
                    catch (Exception e) {
                        throw new PrestoException(NOT_FOUND, "Failed to find/read file: " + tokenFileName, e);
                    }
                }).orElse(Optional.empty());
    }
}
