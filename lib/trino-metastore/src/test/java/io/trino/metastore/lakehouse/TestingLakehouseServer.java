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
package io.trino.metastore.lakehouse;

import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.grpc.GrpcTransportChannel;
import com.google.api.gax.rpc.FixedTransportChannelProvider;
import com.google.cloud.biglake.hive.v1.HiveMetastoreServiceClient;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Duration;

import static java.util.Objects.requireNonNull;

/**
 * Serves a {@link TestingHiveMetastoreService} over gRPC inside the test process and creates
 * clients of the Lakehouse client library that talk to it with the settings of production code.
 */
public final class TestingLakehouseServer
        implements AutoCloseable
{
    private final Server server;
    private final ManagedChannel channel;

    public TestingLakehouseServer(TestingHiveMetastoreService service)
    {
        String name = InProcessServerBuilder.generateName();
        try {
            server = InProcessServerBuilder.forName(name)
                    .directExecutor()
                    .addService(requireNonNull(service, "service is null"))
                    .build()
                    .start();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        channel = InProcessChannelBuilder.forName(name)
                .directExecutor()
                .build();
    }

    public HiveMetastoreServiceClient createClient(Duration requestTimeout)
    {
        try {
            return HiveMetastoreServiceClient.create(GrpcLakehouseClient.serviceSettings(requestTimeout)
                    .setTransportChannelProvider(FixedTransportChannelProvider.create(GrpcTransportChannel.create(channel)))
                    .setCredentialsProvider(NoCredentialsProvider.create())
                    .build());
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void close()
    {
        channel.shutdownNow();
        server.shutdownNow();
    }
}
