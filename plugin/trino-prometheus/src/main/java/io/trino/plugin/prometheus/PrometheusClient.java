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
package io.trino.plugin.prometheus;

import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import io.airlift.http.client.HttpUriBuilder;
import io.airlift.json.JsonCodec;
import io.trino.spi.TrinoException;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import okhttp3.Credentials;
import okhttp3.Interceptor;
import okhttp3.OkHttpClient;
import okhttp3.OkHttpClient.Builder;
import okhttp3.Request;
import okhttp3.Response;

import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

import static com.google.common.base.Verify.verify;
import static com.google.common.net.HttpHeaders.AUTHORIZATION;
import static io.trino.plugin.prometheus.PrometheusErrorCode.PROMETHEUS_TABLES_METRICS_RETRIEVE_ERROR;
import static io.trino.plugin.prometheus.PrometheusErrorCode.PROMETHEUS_UNKNOWN_ERROR;
import static io.trino.spi.StandardErrorCode.GENERIC_USER_ERROR;
import static io.trino.spi.type.TimestampWithTimeZoneType.createTimestampWithTimeZoneType;
import static io.trino.spi.type.TypeSignature.mapType;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.Files.readString;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class PrometheusClient
{
    static final Type TIMESTAMP_COLUMN_TYPE = createTimestampWithTimeZoneType(3);
    static final String METRICS_ENDPOINT = "/api/v1/label/__name__/values";

    private final OkHttpClient httpClient;
    private final Supplier<Map<String, Object>> tableSupplier;
    private final Type varcharMapType;
    private final boolean caseInsensitiveNameMatching;

    @Inject
    public PrometheusClient(PrometheusConnectorConfig config, JsonCodec<Map<String, Object>> metricCodec, TypeManager typeManager)
    {
        requireNonNull(metricCodec, "metricCodec is null");
        requireNonNull(typeManager, "typeManager is null");

        Builder clientBuilder = new Builder().readTimeout(Duration.ofMillis(config.getReadTimeout().toMillis()));
        setupBasicAuth(clientBuilder, config.getUser(), config.getPassword());
        setupTokenAuth(clientBuilder, getBearerAuthInfoFromFile(config.getBearerTokenFile()));
        this.httpClient = clientBuilder.build();

        URI prometheusMetricsUri = getPrometheusMetricsURI(config.getPrometheusURI());
        tableSupplier = Suppliers.memoizeWithExpiration(
                () -> fetchMetrics(metricCodec, prometheusMetricsUri),
                config.getCacheDuration().toMillis(),
                MILLISECONDS);
        varcharMapType = typeManager.getType(mapType(VARCHAR.getTypeSignature(), VARCHAR.getTypeSignature()));
        this.caseInsensitiveNameMatching = config.isCaseInsensitiveNameMatching();
    }

    private static URI getPrometheusMetricsURI(URI prometheusUri)
    {
        // endpoint to retrieve metric names from Prometheus
        return HttpUriBuilder.uriBuilderFrom(prometheusUri).appendPath(METRICS_ENDPOINT).build();
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
        throw new TrinoException(PROMETHEUS_TABLES_METRICS_RETRIEVE_ERROR, "Prometheus did no return metrics list (table names): " + status);
    }

    @Nullable
    public PrometheusTable getTable(String schema, String tableName)
    {
        requireNonNull(schema, "schema is null");
        requireNonNull(tableName, "tableName is null");
        if (!schema.equals("default")) {
            return null;
        }

        String remoteTableName = toRemoteTableName(tableName);
        if (remoteTableName == null) {
            return null;
        }
        return new PrometheusTable(
                remoteTableName,
                ImmutableList.of(
                        new PrometheusColumn("labels", varcharMapType),
                        new PrometheusColumn("timestamp", TIMESTAMP_COLUMN_TYPE),
                        new PrometheusColumn("value", DoubleType.DOUBLE)));
    }

    @Nullable
    private String toRemoteTableName(String tableName)
    {
        verify(tableName.equals(tableName.toLowerCase(ENGLISH)), "tableName not in lower-case: %s", tableName);
        List<String> tableNames = (List<String>) tableSupplier.get().get("data");
        if (tableNames == null) {
            return null;
        }

        if (!caseInsensitiveNameMatching) {
            if (tableNames.contains(tableName)) {
                return tableName;
            }
        }
        else {
            for (String remoteTableName : tableNames) {
                if (tableName.equals(remoteTableName.toLowerCase(ENGLISH))) {
                    return remoteTableName;
                }
            }
        }

        return null;
    }

    private Map<String, Object> fetchMetrics(JsonCodec<Map<String, Object>> metricsCodec, URI metadataUri)
    {
        return metricsCodec.fromJson(fetchUri(metadataUri));
    }

    public byte[] fetchUri(URI uri)
    {
        Request.Builder requestBuilder = new Request.Builder().url(uri.toString());
        try (Response response = httpClient.newCall(requestBuilder.build()).execute()) {
            if (response.isSuccessful() && response.body() != null) {
                return response.body().bytes();
            }
            throw new TrinoException(PROMETHEUS_UNKNOWN_ERROR, "Bad response " + response.code() + " " + response.message());
        }
        catch (IOException e) {
            throw new TrinoException(PROMETHEUS_UNKNOWN_ERROR, "Error reading metrics", e);
        }
    }

    private Optional<String> getBearerAuthInfoFromFile(Optional<File> bearerTokenFile)
    {
        return bearerTokenFile.map(tokenFileName -> {
            try {
                return readString(tokenFileName.toPath(), UTF_8);
            }
            catch (IOException e) {
                throw new TrinoException(PROMETHEUS_UNKNOWN_ERROR, "Failed to read bearer token file: " + tokenFileName, e);
            }
        });
    }

    private static void setupBasicAuth(OkHttpClient.Builder clientBuilder, Optional<String> user, Optional<String> password)
    {
        if (user.isPresent() && password.isPresent()) {
            clientBuilder.addInterceptor(basicAuth(user.get(), password.get()));
        }
    }

    private static void setupTokenAuth(OkHttpClient.Builder clientBuilder, Optional<String> accessToken)
    {
        accessToken.ifPresent(token -> clientBuilder.addInterceptor(tokenAuth(token)));
    }

    private static Interceptor basicAuth(String user, String password)
    {
        requireNonNull(user, "user is null");
        requireNonNull(password, "password is null");
        if (user.contains(":")) {
            throw new TrinoException(GENERIC_USER_ERROR, "Illegal character ':' found in username");
        }

        String credential = Credentials.basic(user, password);
        return chain -> chain.proceed(chain.request().newBuilder()
                .header(AUTHORIZATION, credential)
                .build());
    }

    private static Interceptor tokenAuth(String accessToken)
    {
        requireNonNull(accessToken, "accessToken is null");
        return chain -> chain.proceed(chain.request().newBuilder()
                .addHeader(AUTHORIZATION, "Bearer " + accessToken)
                .build());
    }
}
