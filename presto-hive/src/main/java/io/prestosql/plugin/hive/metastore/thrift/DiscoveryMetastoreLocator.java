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
package io.prestosql.plugin.hive.metastore.thrift;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.net.HostAndPort;
import com.orbitz.consul.Consul;
import com.orbitz.consul.HealthClient;
import com.orbitz.consul.model.ConsulResponse;
import com.orbitz.consul.model.health.ServiceHealth;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import io.prestosql.spi.PrestoException;

import javax.inject.Inject;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;
import static io.prestosql.plugin.hive.HiveErrorCode.HIVE_METASTORE_ERROR;

public class DiscoveryMetastoreLocator
        implements MetastoreLocator
{
    private static final Logger log = Logger.get(DiscoveryMetastoreLocator.class);
    private static final String CONSUL_SCHEME = "consul";
    private static final String THRIFT_SCHEME = "thrift";

    private final HiveMetastoreClientFactory clientFactory;
    private final List<URI> unresolvedUris;
    private final String metastoreUsername;

    private final Supplier<List<HostAndPort>> resolvedUriSupplier;

    public DiscoveryMetastoreLocator(List<URI> unresolvedUris, Duration resolvedUrisTtl, String metastoreUsername, HiveMetastoreClientFactory clientFactory)
    {
        this.clientFactory = clientFactory;
        this.unresolvedUris = unresolvedUris;
        this.metastoreUsername = metastoreUsername;

        // basic error checks
        checkArgument(!unresolvedUris.isEmpty(), "metastoreUris must specify at least one URI");
        unresolvedUris.forEach(DiscoveryMetastoreLocator::checkMetastoreUri);

        final Duration ttl = resolvedUrisTtl;
        this.resolvedUriSupplier = Suppliers.memoizeWithExpiration(
                () -> resolveUris(unresolvedUris),
                Math.round(ttl.getValue(TimeUnit.MILLISECONDS)),
                TimeUnit.MILLISECONDS);
    }

    @Inject
    public DiscoveryMetastoreLocator(DiscoveryMetastoreConfig config, HiveMetastoreClientFactory clientFactory)
    {
        this(config.getMetastoreUris(), config.getResolvedUrisTtl(), config.getMetastoreUsername(), clientFactory);
    }

    protected static void checkMetastoreUri(URI uri)
    {
        String scheme = uri.getScheme();
        checkArgument(!isNullOrEmpty(scheme), "metastoreUri scheme is missing: %s", uri);
        if (scheme.equalsIgnoreCase(CONSUL_SCHEME)) {
            checkArgument(!isNullOrEmpty(uri.getHost()), "Unspecified consul host, please use consul://consul-host:consul-port/service-name");
            checkArgument(uri.getPort() != -1, "Unspecified consul port, please use consul://consul-host:consul-port/service-name");
            checkArgument(!isNullOrEmpty(uri.getPath()), "Unspecified consul service, please use consul://consul-host:consul-port/service-name");
        }
        else {
            checkArgument(!isNullOrEmpty(scheme), "metastoreUri scheme is missing: %s", uri);
            checkArgument(scheme.equals(THRIFT_SCHEME), "metastoreUri scheme must be thrift: %s", uri);
            checkArgument(uri.getHost() != null, "metastoreUri host is missing: %s", uri);
            checkArgument(uri.getPort() != -1, "metastoreUri port is missing: %s", uri);
        }
    }

    @Override
    public ThriftMetastoreClient createMetastoreClient()
    {
        final List<HostAndPort> resolvedUris = new ArrayList<>(resolvedUriSupplier.get());
        Collections.shuffle(resolvedUris);

        Exception lastException = null;

        for (HostAndPort host : resolvedUris) {
            log.info("Connecting to metastore %s:%d", host.getHost(), host.getPort());
            try {
                final ThriftMetastoreClient client = clientFactory.create(host);
                if (!isNullOrEmpty(metastoreUsername)) {
                    client.setUGI(metastoreUsername);
                }
                return client;
            }
            catch (Exception e) {
                log.warn("Failed to connect to metastore: " + host.toString());
                lastException = e;
            }
        }
        throw new PrestoException(HIVE_METASTORE_ERROR,
                "Failed connecting to Hive metastore using any of the URI's: " + unresolvedUris, lastException);
    }

    private static List<HostAndPort> resolveUris(List<URI> unresolvedUris)
    {
        final ImmutableList.Builder<HostAndPort> results = ImmutableList.builder();
        for (URI uri : unresolvedUris) {
            final String scheme = uri.getScheme();
            if (scheme.equalsIgnoreCase(CONSUL_SCHEME)) {
                try {
                    final List<HostAndPort> resolved = resolveUsingConsul(uri);
                    results.addAll(resolved);
                    if (!resolved.isEmpty()) {
                        // if we can resolve with consul we don't go further down the list (fallbacks uris)
                        break;
                    }
                }
                catch (Exception e) {
                    log.warn("Error resolving consul uri: " + uri, e);
                }
            }
            else {
                results.add(HostAndPort.fromParts(uri.getHost(), uri.getPort()));
            }
        }
        return results.build();
    }

    private static List<HostAndPort> resolveUsingConsul(URI consulUri)
    {
        log.info("Resolving consul uri %s", consulUri);
        final String consulHost = consulUri.getHost();
        final String service = consulUri.getPath().substring(1);  //strip leading slash
        final int consulPort = consulUri.getPort();
        final HostAndPort hostAndPort = HostAndPort.fromParts(consulHost, consulPort);

        final Consul consul = Consul.builder().withHostAndPort(hostAndPort).build();
        final HealthClient healthClient = consul.healthClient();
        final ConsulResponse<List<ServiceHealth>> result = healthClient.getHealthyServiceInstances(service);

        if (result == null) {
            throw new PrestoException(HIVE_METASTORE_ERROR, "Unable to query " + consulUri + " for service " + service);
        }

        return result.getResponse().stream().map(uri -> {
            String host = uri.getNode().getNode();
            int port = uri.getService().getPort();
            return HostAndPort.fromParts(host, port);
        }).collect(Collectors.toList());
    }
}
