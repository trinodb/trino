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

import com.google.common.cache.CacheLoader;
import com.google.common.net.HostAndPort;
import io.airlift.units.Duration;
import io.prestosql.plugin.hive.util.ObjectPool;

import javax.annotation.PreDestroy;
import javax.inject.Inject;
import javax.inject.Qualifier;

import java.io.Closeable;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import java.lang.reflect.InvocationTargetException;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.reflect.Reflection.newProxy;
import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.RetentionPolicy.RUNTIME;
import static java.util.Objects.requireNonNull;

public class PooledHiveMetastoreClientFactory
        implements ThriftMetastoreClientFactory, Closeable
{
    private final ObjectPool<Key, ThriftMetastoreClient> pool;

    @Inject
    public PooledHiveMetastoreClientFactory(@ForPooledMetastore ThriftMetastoreClientFactory delegate)
    {
        this.pool = new ObjectPool<>(
                new CacheLoader<>()
                {
                    @Override
                    public ThriftMetastoreClient load(Key key)
                            throws Exception
                    {
                        return delegate.create(key.getAddress(), key.getDelegationToken());
                    }
                },
                1000,
                Duration.valueOf("30s"));
    }

    @Override
    public ThriftMetastoreClient create(HostAndPort address, Optional<String> delegationToken)
    {
        ObjectPool.Lease<ThriftMetastoreClient> lease = pool.get(new Key(delegationToken, address));
        return newProxy(ThriftMetastoreClient.class, (proxy, method, args) -> {
            if (method.getName().equals("close")) {
                lease.close();
                return null;
            }
            try {
                return method.invoke(lease.get(), args);
            }
            catch (InvocationTargetException e) {
                lease.invalidate();
                throw e.getCause();
            }
        });
    }

    @PreDestroy
    @Override
    public void close()
    {
        pool.close();
    }

    @Retention(RUNTIME)
    @Target({FIELD, PARAMETER, METHOD})
    @Qualifier
    public @interface ForPooledMetastore
    {
    }

    private static class Key
    {
        private final Optional<String> delegationToken;
        private final HostAndPort address;

        private Key(Optional<String> delegationToken, HostAndPort address)
        {
            this.delegationToken = requireNonNull(delegationToken, "delegationToken is null");
            this.address = requireNonNull(address, "address is null");
        }

        public Optional<String> getDelegationToken()
        {
            return delegationToken;
        }

        public HostAndPort getAddress()
        {
            return address;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Key key = (Key) o;
            return Objects.equals(delegationToken, key.delegationToken) &&
                    Objects.equals(address, key.address);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(delegationToken, address);
        }
    }
}
