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

import io.trino.plugin.base.security.UserNameProvider;
import io.trino.plugin.hive.ForHiveMetastore;
import io.trino.spi.security.ConnectorIdentity;
import org.apache.thrift.TException;

import javax.inject.Inject;

import java.io.Closeable;
import java.io.IOException;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class UgiBasedMetastoreClientFactory
        implements IdentityAwareMetastoreClientFactory
{
    private final TokenAwareMetastoreClientFactory clientProvider;
    private final UserNameProvider userNameProvider;
    private final boolean impersonationEnabled;

    @Inject
    public UgiBasedMetastoreClientFactory(
            TokenAwareMetastoreClientFactory clientProvider,
            @ForHiveMetastore UserNameProvider userNameProvider,
            ThriftMetastoreConfig thriftConfig)
    {
        this.clientProvider = requireNonNull(clientProvider, "clientProvider is null");
        this.userNameProvider = requireNonNull(userNameProvider, "userNameProvider is null");
        this.impersonationEnabled = thriftConfig.isImpersonationEnabled();
    }

    @Override
    public ThriftMetastoreClient createMetastoreClientFor(Optional<ConnectorIdentity> identity)
            throws TException
    {
        ThriftMetastoreClient client = clientProvider.createMetastoreClient(Optional.empty());

        if (impersonationEnabled) {
            String username = identity.map(userNameProvider::get)
                    .orElseThrow(() -> new IllegalStateException("End-user name should exist when metastore impersonation is enabled"));
            setMetastoreUserOrClose(client, username);
        }

        return client;
    }

    private static void setMetastoreUserOrClose(ThriftMetastoreClient client, String username)
            throws TException
    {
        try {
            client.setUGI(username);
        }
        catch (Throwable t) {
            // close client and suppress any error from close
            try (Closeable ignored = client) {
                throw t;
            }
            catch (IOException e) {
                // impossible; will be suppressed
            }
        }
    }
}
