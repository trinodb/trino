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
package io.trino.plugin.hive.metastore.glue;

import com.google.inject.Inject;
import io.opentelemetry.api.trace.Tracer;
import io.trino.metastore.HiveMetastore;
import io.trino.metastore.HiveMetastoreFactory;
import io.trino.metastore.tracing.TracingHiveMetastore;
import io.trino.spi.security.ConnectorIdentity;

import java.util.Optional;

public class GlueHiveMetastoreFactory
        implements HiveMetastoreFactory
{
    private final HiveMetastore metastore;

    // Glue metastore does not support impersonation, so just use single shared instance
    @Inject
    public GlueHiveMetastoreFactory(GlueHiveMetastore metastore, Tracer tracer)
    {
        this.metastore = new TracingHiveMetastore(tracer, metastore);
    }

    @Override
    public boolean hasBuiltInCaching()
    {
        return true;
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
}
