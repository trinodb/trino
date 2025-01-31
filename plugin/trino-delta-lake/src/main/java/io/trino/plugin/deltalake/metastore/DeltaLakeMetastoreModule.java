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
package io.trino.plugin.deltalake.metastore;

import com.google.inject.Binder;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.plugin.deltalake.metastore.file.DeltaLakeFileMetastoreModule;
import io.trino.plugin.deltalake.metastore.glue.DeltaLakeGlueMetastoreModule;
import io.trino.plugin.deltalake.metastore.glue.v1.DeltaLakeGlueV1MetastoreModule;
import io.trino.plugin.deltalake.metastore.thrift.DeltaLakeThriftMetastoreModule;
import io.trino.plugin.hive.metastore.CachingHiveMetastoreModule;
import io.trino.plugin.hive.metastore.MetastoreTypeConfig;

public class DeltaLakeMetastoreModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        install(switch (buildConfigObject(MetastoreTypeConfig.class).getMetastoreType()) {
            case THRIFT -> new DeltaLakeThriftMetastoreModule();
            case FILE -> new DeltaLakeFileMetastoreModule();
            case GLUE -> new DeltaLakeGlueMetastoreModule();
            case GLUE_V1 -> new DeltaLakeGlueV1MetastoreModule();
        });

        install(new CachingHiveMetastoreModule());
    }
}
