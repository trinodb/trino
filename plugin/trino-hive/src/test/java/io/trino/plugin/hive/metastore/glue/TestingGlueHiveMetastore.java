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

import io.opentelemetry.api.OpenTelemetry;
import io.trino.plugin.hive.metastore.glue.GlueHiveMetastore.TableKind;

import java.nio.file.Path;
import java.util.EnumSet;

import static io.trino.plugin.hive.HiveTestUtils.HDFS_FILE_SYSTEM_FACTORY;
import static io.trino.plugin.hive.metastore.glue.GlueMetastoreModule.createGlueClient;

public final class TestingGlueHiveMetastore
{
    private TestingGlueHiveMetastore() {}

    public static GlueHiveMetastore createTestingGlueHiveMetastore(Path defaultWarehouseDir)
    {
        GlueHiveMetastoreConfig glueConfig = new GlueHiveMetastoreConfig()
                .setDefaultWarehouseDir(defaultWarehouseDir.toUri().toString());
        return new GlueHiveMetastore(
                createGlueClient(glueConfig, OpenTelemetry.noop()),
                new GlueContext(glueConfig),
                GlueCache.NOOP,
                HDFS_FILE_SYSTEM_FACTORY,
                glueConfig,
                EnumSet.allOf(TableKind.class));
    }
}
