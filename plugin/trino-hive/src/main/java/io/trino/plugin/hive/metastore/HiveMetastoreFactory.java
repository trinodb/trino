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

import static java.util.Objects.requireNonNull;

public interface HiveMetastoreFactory
{
    HiveMetastore createMetastore();

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
        public HiveMetastore createMetastore()
        {
            return metastore;
        }
    }
}
