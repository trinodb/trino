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

import com.google.api.gax.grpc.InstantiatingGrpcChannelProvider;
import com.google.api.gax.rpc.HeaderProvider;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.inject.Inject;
import io.trino.spi.connector.ConnectorSession;

import static java.util.Objects.requireNonNull;

public class HeaderOptionsConfigurer
        implements BigQueryGrpcOptionsConfigurer
{
    private final HeaderProvider headerProvider;

    @Inject
    public HeaderOptionsConfigurer(HeaderProvider headerProvider)
    {
        this.headerProvider = requireNonNull(headerProvider, "headerProvider is null");
    }

    @Override
    public BigQueryOptions.Builder configure(BigQueryOptions.Builder builder, ConnectorSession session)
    {
        return builder.setHeaderProvider(headerProvider);
    }

    @Override
    public InstantiatingGrpcChannelProvider.Builder configure(InstantiatingGrpcChannelProvider.Builder channelBuilder, ConnectorSession session)
    {
        return channelBuilder.setHeaderProvider(headerProvider);
    }
}
