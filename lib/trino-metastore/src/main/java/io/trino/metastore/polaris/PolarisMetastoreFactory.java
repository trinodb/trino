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
package io.trino.metastore.polaris;

import com.google.errorprone.annotations.concurrent.GuardedBy;
import com.google.inject.Inject;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.metastore.HiveMetastore;
import io.trino.metastore.HiveMetastoreFactory;
import io.trino.spi.security.ConnectorIdentity;
import org.apache.iceberg.rest.RESTSessionCatalog;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class PolarisMetastoreFactory
        implements HiveMetastoreFactory
{
    private final PolarisRestClient polarisClient;
    private final RESTSessionCatalog restSessionCatalog;
    private final SecurityProperties securityProperties;
    private final PolarisMetastoreStats stats;
    private final TrinoFileSystemFactory fileSystemFactory;

    @GuardedBy("this")
    private PolarisMetastore metastore;

    @Inject
    public PolarisMetastoreFactory(
            PolarisRestClient polarisClient,
            RESTSessionCatalog restSessionCatalog,
            SecurityProperties securityProperties,
            PolarisMetastoreStats stats,
            TrinoFileSystemFactory fileSystemFactory)
    {
        this.polarisClient = requireNonNull(polarisClient, "polarisClient is null");
        this.restSessionCatalog = requireNonNull(restSessionCatalog, "restSessionCatalog is null");
        this.securityProperties = requireNonNull(securityProperties, "securityProperties is null");
        this.stats = requireNonNull(stats, "stats is null");
        this.fileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
    }

    @Override
    public boolean hasBuiltInCaching()
    {
        return false;
    }

    @Override
    public boolean isImpersonationEnabled()
    {
        return false;
    }

    @Override
    public synchronized HiveMetastore createMetastore(Optional<ConnectorIdentity> identity)
    {
        if (metastore == null) {
            metastore = new PolarisMetastore(polarisClient, restSessionCatalog, securityProperties, stats, fileSystemFactory);
        }
        return metastore;
    }
}
