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
import io.trino.plugin.deltalake.util.DockerizedCephDataLake;
import io.trino.plugin.deltalake.util.DockerizedDataLake;
import io.trino.plugin.hive.parquet.ParquetWriterConfig;
import io.trino.testing.QueryRunner;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.deltalake.DeltaLakeQueryRunner.DELTA_CATALOG;
import static io.trino.plugin.deltalake.util.CephContainer.CEPH_ACCESS_KEY;
import static io.trino.plugin.deltalake.util.CephContainer.CEPH_PORT;
import static io.trino.plugin.deltalake.util.CephContainer.CEPH_SECRET_KEY;
import static io.trino.plugin.deltalake.util.TestingHadoop.renderHadoopCoreSiteTemplate;
import static java.lang.String.format;

public class TestDeltaLakeCephConnectorSmokeTest
        extends BaseDeltaLakeConnectorSmokeTest
{
    protected DockerizedCephDataLake dockerizedCephDataLake;

    @Override
    protected DockerizedDataLake createDockerizedDataLake()
            throws IOException
    {
        Path hadoopCoreSiteXmlTempFile = renderHadoopCoreSiteTemplate(
                "io/trino/plugin/deltalake/core-site.xml",
                ImmutableMap.of(
                        "%S3_ENDPOINT%", format("http://ceph:%s", CEPH_PORT),
                        "%S3_ACCESS_KEY%", CEPH_ACCESS_KEY,
                        "%S3_SECRET_KEY%", CEPH_SECRET_KEY));

        dockerizedCephDataLake = new DockerizedCephDataLake(
                bucketName,
                getHadoopBaseImage(),
                ImmutableMap.of(),
                ImmutableMap.of(hadoopCoreSiteXmlTempFile.normalize().toAbsolutePath().toString(), "/etc/hadoop/conf/core-site.xml"));
        return dockerizedCephDataLake;
    }

    @Override
    protected QueryRunner createDeltaLakeQueryRunner(Map<String, String> connectorProperties)
            throws Exception
    {
        verify(!new ParquetWriterConfig().isParquetOptimizedWriterEnabled(), "This test assumes the optimized Parquet writer is disabled by default");
        return DeltaLakeQueryRunner.createCephDeltaLakeQueryRunner(
                DELTA_CATALOG,
                SCHEMA,
                ImmutableMap.<String, String>builder()
                        .putAll(connectorProperties)
                        .put("delta.enable-non-concurrent-writes", "true")
                        .buildOrThrow(),
                dockerizedCephDataLake.getCephEndpoint(),
                dockerizedCephDataLake.getTestingHadoop());
    }

    @Override
    protected void createTableFromResources(String table, String resourcePath, QueryRunner queryRunner)
    {
        dockerizedCephDataLake.copyResources(resourcePath, table);
        queryRunner.execute(format("CREATE TABLE %s (dummy int) WITH (location = '%s')",
                table,
                getLocationForTable(bucketName, table)));
    }

    @Override
    protected String getLocationForTable(String bucketName, String tableName)
    {
        return format("s3://%s/%s", bucketName, tableName);
    }

    @Override
    protected List<String> getTableFiles(String tableName)
    {
        return dockerizedCephDataLake.listFiles(tableName).stream()
                .map(path -> format("s3://%s/%s", bucketName, path))
                .collect(toImmutableList());
    }

    @Override
    protected List<String> listCheckpointFiles(String transactionLogDirectory)
    {
        return dockerizedCephDataLake.listFiles(transactionLogDirectory)
                .stream()
                .filter(path -> path.contains("checkpoint.parquet"))
                .map(path -> format("s3://%s/%s", bucketName, path))
                .collect(toImmutableList());
    }

    @Override
    protected String bucketUrl()
    {
        return format("s3://%s/", bucketName);
    }
}
