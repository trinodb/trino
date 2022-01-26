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
package io.trino.plugin.deltalake;

import com.google.common.collect.ImmutableMap;
import io.trino.plugin.deltalake.util.DockerizedDataLake;
import io.trino.plugin.deltalake.util.DockerizedMinioDataLake;
import io.trino.testing.QueryRunner;

import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.String.format;

public abstract class BaseDeltaLakeAwsConnectorSmokeTest
        extends BaseDeltaLakeConnectorSmokeTest
{
    protected DockerizedMinioDataLake dockerizedMinioDataLake;

    @Override
    DockerizedDataLake createDockerizedDataLake()
    {
        dockerizedMinioDataLake = new DockerizedMinioDataLake(
                bucketName,
                getHadoopBaseImage(),
                ImmutableMap.of("io/trino/plugin/deltalake/core-site.xml", "/etc/hadoop/conf/core-site.xml"),
                ImmutableMap.of());
        return dockerizedMinioDataLake;
    }

    @Override
    void createTableFromResources(String table, String resourcePath, QueryRunner queryRunner)
    {
        dockerizedMinioDataLake.copyResources(resourcePath, table);
        queryRunner.execute(format("CREATE TABLE %s (dummy int) WITH (location = '%s')",
                table,
                getLocationForTable(bucketName, table)));
    }

    @Override
    String getLocationForTable(String bucketName, String tableName)
    {
        return format("s3://%s/%s", bucketName, tableName);
    }

    @Override
    List<String> getTableFiles(String tableName)
    {
        return dockerizedMinioDataLake.listFiles(tableName).stream()
                .map(path -> format("s3://%s/%s", bucketName, path))
                .collect(toImmutableList());
    }

    @Override
    protected List<String> listCheckpointFiles(String transactionLogDirectory)
    {
        return dockerizedMinioDataLake.listFiles(transactionLogDirectory)
                .stream()
                .filter(path -> path.contains("checkpoint.parquet"))
                .map(path -> format("s3://%s/%s", bucketName, path))
                .collect(toImmutableList());
    }
}
