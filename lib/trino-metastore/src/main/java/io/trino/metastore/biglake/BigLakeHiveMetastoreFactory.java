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
package io.trino.metastore.biglake;

import com.google.cloud.biglake.hive.v1.HiveMetastoreServiceClient;
import com.google.common.base.Suppliers;
import io.opentelemetry.api.trace.Tracer;
import io.trino.metastore.HiveMetastore;
import io.trino.metastore.HiveMetastoreFactory;
import io.trino.metastore.tracing.TracingHiveMetastore;
import io.trino.spi.TrinoException;
import io.trino.spi.security.ConnectorIdentity;

import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;
import java.util.Optional;
import java.util.function.Supplier;

import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.util.Objects.requireNonNull;

/**
 * Builds the BigLake metastore and owns the lifecycle of the BigLake client behind it.
 *
 * <p>BigLake authenticates the process rather than the query, so there is one shared metastore and
 * no impersonation.
 *
 * <p>The client is built on first use rather than on creation. Building it resolves Google
 * credentials, and a catalog that cannot reach BigLake fails the queries that need it instead of
 * stopping the server from starting.
 */
public class BigLakeHiveMetastoreFactory
        implements HiveMetastoreFactory, Closeable
{
    private final String projectId;
    private final String catalogId;
    private final Supplier<HiveMetastoreServiceClient> serviceClientFactory;
    private final HiveMetastore metastore;

    private volatile GrpcBigLakeClient client;

    public BigLakeHiveMetastoreFactory(BigLakeMetastoreConfig config, Tracer tracer)
    {
        this(requireNonNull(config, "config is null").getProjectId(),
                config.getCatalogId(),
                tracer,
                serviceClientFactory(Duration.ofMillis(config.getRequestTimeout().toMillis())));
    }

    BigLakeHiveMetastoreFactory(
            String projectId,
            String catalogId,
            Tracer tracer,
            Supplier<HiveMetastoreServiceClient> serviceClientFactory)
    {
        this.projectId = requireNonNull(projectId, "projectId is null");
        this.catalogId = requireNonNull(catalogId, "catalogId is null");
        this.serviceClientFactory = requireNonNull(serviceClientFactory, "serviceClientFactory is null");
        requireNonNull(tracer, "tracer is null");

        this.metastore = new TracingHiveMetastore(
                tracer,
                new BigLakeHiveMetastore(Suppliers.memoize(this::createClient)));
    }

    @Override
    public boolean isImpersonationEnabled()
    {
        return false;
    }

    @Override
    public HiveMetastore createMetastore(Optional<ConnectorIdentity> identity)
    {
        return metastore;
    }

    private BigLakeClient createClient()
    {
        GrpcBigLakeClient created = new GrpcBigLakeClient(serviceClientFactory.get(), projectId, catalogId);
        client = created;
        return created;
    }

    private static Supplier<HiveMetastoreServiceClient> serviceClientFactory(Duration requestTimeout)
    {
        return () -> {
            try {
                return HiveMetastoreServiceClient.create(GrpcBigLakeClient.serviceSettings(requestTimeout).build());
            }
            catch (IOException e) {
                throw new TrinoException(GENERIC_INTERNAL_ERROR, "Failed to connect to BigLake", e);
            }
        };
    }

    @Override
    public void close()
    {
        GrpcBigLakeClient created = client;
        if (created == null) {
            return;
        }
        client = null;
        try {
            created.close();
        }
        catch (RuntimeException e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Failed to close the BigLake client", e);
        }
    }
}
