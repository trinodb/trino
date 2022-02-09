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
package io.trino.plugin.hive.metastore;

import io.trino.spi.security.ConnectorIdentity;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public interface HiveMetastoreFactory
{
    boolean isImpersonationEnabled();

    /**
     * Create a metastore instance for the identity. An empty identity will
     * only be provided when impersonation is disabled, and global caching is
     * enabled.
     */
    HiveMetastore createMetastore(Optional<ConnectorIdentity> identity);

    static HiveMetastoreFactory ofInstance(HiveMetastore metastore)
    {
        return new StaticHiveMetastoreFactory(metastore);
    }

    class StaticHiveMetastoreFactory
            implements HiveMetastoreFactory
    {
        private final HiveMetastore metastore;

        private StaticHiveMetastoreFactory(HiveMetastore metastore)
        {
            this.metastore = requireNonNull(metastore, "metastore is null");
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
}
