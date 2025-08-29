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
package io.trino.plugin.jdbc;

import io.trino.plugin.base.cache.identity.IdentityCacheMapping;
import io.trino.spi.connector.ConnectorSession;

public final class SingletonIdentityCacheMapping
        implements IdentityCacheMapping
{
    @Override
    public IdentityCacheKey getRemoteUserCacheKey(ConnectorSession session)
    {
        return SingletonIdentityCacheKey.INSTANCE;
    }

    private static final class SingletonIdentityCacheKey
            extends IdentityCacheKey
    {
        private static final SingletonIdentityCacheKey INSTANCE = new SingletonIdentityCacheKey();

        @Override
        public int hashCode()
        {
            return getClass().hashCode();
        }

        @Override
        public boolean equals(Object obj)
        {
            return obj instanceof SingletonIdentityCacheKey;
        }
    }
}
