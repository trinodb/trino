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

import io.trino.plugin.hive.metastore.HiveMetastore;
import io.trino.plugin.hive.metastore.HiveMetastoreFactory;
import org.weakref.jmx.Flatten;
import org.weakref.jmx.Managed;

import javax.inject.Inject;

import static java.util.Objects.requireNonNull;

public class GlueHiveMetastoreFactory
        implements HiveMetastoreFactory
{
    private final GlueHiveMetastore metastore;

    // Glue metastore does not support impersonation, so just use single shared instance
    @Inject
    public GlueHiveMetastoreFactory(GlueHiveMetastore metastore)
    {
        this.metastore = requireNonNull(metastore, "metastore is null");
    }

    @Flatten
    @Managed
    public GlueHiveMetastore getMetastore()
    {
        return metastore;
    }

    @Override
    public HiveMetastore createMetastore()
    {
        return metastore;
    }
}
