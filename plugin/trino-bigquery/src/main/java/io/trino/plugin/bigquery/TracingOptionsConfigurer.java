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
package io.trino.plugin.bigquery;

import com.google.api.client.http.apache.v2.ApacheHttpTransport;
import com.google.api.gax.grpc.GrpcInterceptorProvider;
import com.google.api.gax.grpc.InstantiatingGrpcChannelProvider;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.http.HttpTransportOptions;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.instrumentation.apachehttpclient.v4_3.ApacheHttpClientTelemetry;
import io.opentelemetry.instrumentation.grpc.v1_6.GrpcTelemetry;
import io.trino.spi.connector.ConnectorSession;

import static java.util.Objects.requireNonNull;

public class TracingOptionsConfigurer
        implements BigQueryGrpcOptionsConfigurer
{
    private final OpenTelemetry openTelemetry;
    private final GrpcInterceptorProvider grpcInterceptorProvider;

    @Inject
    public TracingOptionsConfigurer(OpenTelemetry openTelemetry)
    {
        this.openTelemetry = requireNonNull(openTelemetry, "openTelemetry is null");
        this.grpcInterceptorProvider = () -> ImmutableList.of(GrpcTelemetry.create(openTelemetry).newClientInterceptor());
    }

    @Override
    public BigQueryOptions.Builder configure(BigQueryOptions.Builder builder, ConnectorSession session)
    {
        return builder.setTransportOptions(HttpTransportOptions.newBuilder()
                .setHttpTransportFactory(() -> new ApacheHttpTransport(ApacheHttpClientTelemetry.create(openTelemetry).newHttpClient()))
                .build());
    }

    @Override
    public InstantiatingGrpcChannelProvider.Builder configure(InstantiatingGrpcChannelProvider.Builder channelBuilder, ConnectorSession session)
    {
        channelBuilder.setInterceptorProvider(grpcInterceptorProvider);
        return channelBuilder;
    }
}
