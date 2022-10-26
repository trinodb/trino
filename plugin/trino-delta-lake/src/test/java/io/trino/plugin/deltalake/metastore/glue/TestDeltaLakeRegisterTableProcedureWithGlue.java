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
package io.trino.plugin.deltalake.metastore.glue;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.google.common.collect.ImmutableMap;
import io.trino.plugin.deltalake.BaseDeltaLakeRegisterTableProcedureTest;
import io.trino.plugin.hive.metastore.HiveMetastore;
import io.trino.plugin.hive.metastore.glue.DefaultGlueColumnStatisticsProviderFactory;
import io.trino.plugin.hive.metastore.glue.GlueHiveMetastore;
import io.trino.plugin.hive.metastore.glue.GlueHiveMetastoreConfig;

import java.util.Map;
import java.util.Optional;

import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.trino.plugin.hive.HiveTestUtils.HDFS_ENVIRONMENT;

public class TestDeltaLakeRegisterTableProcedureWithGlue
        extends BaseDeltaLakeRegisterTableProcedureTest
{
    @Override
    protected Map<String, String> getConnectorProperties(String dataDirectory)
    {
        return ImmutableMap.of(
                "hive.metastore", "glue",
                "hive.metastore.glue.default-warehouse-dir", dataDirectory);
    }

    @Override
    protected HiveMetastore createTestMetastore(String dataDirectory)
    {
        return new GlueHiveMetastore(
                HDFS_ENVIRONMENT,
                new GlueHiveMetastoreConfig(),
                DefaultAWSCredentialsProviderChain.getInstance(),
                directExecutor(),
                new DefaultGlueColumnStatisticsProviderFactory(directExecutor(), directExecutor()),
                Optional.empty(),
                table -> true);
    }
}
