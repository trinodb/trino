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
package io.trino.plugin.hive.metastore.thrift;

import com.google.inject.Inject;
import io.opentelemetry.api.trace.Tracer;
import io.trino.metastore.HiveMetastore;
import io.trino.metastore.tracing.TracingHiveMetastore;
import io.trino.plugin.hive.metastore.HiveMetastoreFactory;
import io.trino.spi.security.ConnectorIdentity;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class BridgingHiveMetastoreFactory
        implements HiveMetastoreFactory
{
    private final ThriftMetastoreFactory thriftMetastoreFactory;
    private final Tracer tracer;

    @Inject
    public BridgingHiveMetastoreFactory(ThriftMetastoreFactory thriftMetastoreFactory, Tracer tracer)
    {
        this.thriftMetastoreFactory = requireNonNull(thriftMetastoreFactory, "thriftMetastore is null");
        this.tracer = requireNonNull(tracer, "tracer is null");
    }

    @Override
    public boolean isImpersonationEnabled()
    {
        return thriftMetastoreFactory.isImpersonationEnabled();
    }

    @Override
    public HiveMetastore createMetastore(Optional<ConnectorIdentity> identity)
    {
        return new TracingHiveMetastore(tracer,
                new BridgingHiveMetastore(thriftMetastoreFactory.createMetastore(identity)));
    }
}
