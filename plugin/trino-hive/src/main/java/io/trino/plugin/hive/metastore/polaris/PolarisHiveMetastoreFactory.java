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
package io.trino.plugin.hive.metastore.polaris;

import com.google.inject.Inject;
import io.trino.metastore.HiveMetastore;
import io.trino.metastore.HiveMetastoreFactory;
import io.trino.spi.security.ConnectorIdentity;
import org.apache.iceberg.rest.RESTSessionCatalog;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class PolarisHiveMetastoreFactory
        implements HiveMetastoreFactory
{
    private final PolarisRestClient polarisClient;
    private final RESTSessionCatalog restSessionCatalog;
    private final SecurityProperties securityProperties;

    @Inject
    public PolarisHiveMetastoreFactory(PolarisRestClient polarisClient, RESTSessionCatalog restSessionCatalog, SecurityProperties securityProperties)
    {
        this.polarisClient = requireNonNull(polarisClient, "polarisClient is null");
        this.restSessionCatalog = requireNonNull(restSessionCatalog, "restSessionCatalog is null");
        this.securityProperties = requireNonNull(securityProperties, "securityProperties is null");
    }

    @Override
    public HiveMetastore createMetastore(Optional<ConnectorIdentity> identity)
    {
        return new PolarisHiveMetastore(polarisClient, restSessionCatalog, securityProperties);
    }

    @Override
    public boolean isImpersonationEnabled()
    {
        return false;
    }
}
