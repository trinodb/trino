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

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.trino.spi.security.ConnectorIdentity;
import org.apache.thrift.TException;

import java.net.URI;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class StaticTokenAwareHttpMetastoreClientFactory
        implements IdentityAwareMetastoreClientFactory
{
    private final ThriftMetastoreClientFactory clientProvider;
    private final ImmutableList<URI> metastoreUris;

    @Inject
    StaticTokenAwareHttpMetastoreClientFactory(StaticMetastoreConfig config, ThriftMetastoreClientFactory clientProvider)
    {
        this.clientProvider = requireNonNull(clientProvider, "clientProvider is null");
        this.metastoreUris = config.getMetastoreUris().stream()
                .map(StaticTokenAwareHttpMetastoreClientFactory::checkMetastoreUri)
                .collect(toImmutableList());
    }

    private static URI checkMetastoreUri(URI uri)
    {
        requireNonNull(uri, "uri is null");
        String scheme = uri.getScheme();
        checkArgument(!isNullOrEmpty(scheme), "metastoreUri scheme is missing: %s", uri);
        checkArgument(scheme.equals("https") || scheme.equals("http"), "metastoreUri scheme must be http or https: %s", uri);
        checkArgument(uri.getHost() != null, "metastoreUri host is missing: %s", uri);
        return uri;
    }

    @Override
    public ThriftMetastoreClient createMetastoreClientFor(Optional<ConnectorIdentity> identity)
            throws TException
    {
        TException lastException = null;
        for (URI metastoreUri : metastoreUris) {
            try {
                return clientProvider.create(metastoreUri, Optional.empty());
            }
            catch (TException e) {
                lastException = e;
            }
        }
        throw lastException;
    }
}
