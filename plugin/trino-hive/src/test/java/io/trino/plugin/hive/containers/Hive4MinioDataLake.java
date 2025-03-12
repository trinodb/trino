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
package io.trino.plugin.hive.containers;

import com.google.common.collect.ImmutableMap;

import java.net.URI;
import java.util.Map;
import java.util.Set;

import static io.trino.plugin.hive.containers.Hive4HiveServer.HIVE_SERVER_PORT;
import static io.trino.plugin.hive.containers.Hive4Metastore.HIVE4_IMAGE;
import static io.trino.plugin.hive.containers.HiveMinioDataLake.State.STARTED;
import static io.trino.testing.containers.Minio.MINIO_ACCESS_KEY;
import static io.trino.testing.containers.Minio.MINIO_SECRET_KEY;
import static io.trino.testing.containers.TestContainers.getPathFromClassPathResource;

public class Hive4MinioDataLake
        extends HiveMinioDataLake
{
    private final Hive4HiveServer hiveServer;
    private final Hive4Metastore hiveMetastore;

    public Hive4MinioDataLake(String bucketName)
    {
        super(bucketName);
        String hiveImage = HIVE4_IMAGE;
        Map<String, String> hiveFilesToMount = ImmutableMap.of("/opt/hive/conf/hive-site.xml", getPathFromClassPathResource("hive_minio_datalake/hive4-hive-site.xml"));
        // Separate hms and hiveserver(below) is created as standalone hiveserver doesn't expose embedded hms. https://github.com/apache/hive/blob/a1420ed816c315d98be7ebf05cdc3ba139a68643/packaging/src/docker/README.md?plain=1#L46.
        // Run standalone metastore https://github.com/apache/hive/blob/a1420ed816c315d98be7ebf05cdc3ba139a68643/packaging/src/docker/README.md?plain=1#L105
        Hive4Metastore.Builder metastorebuilder = Hive4Metastore.builder()
                .withImage(hiveImage)
                .withEnvVars(Map.of("SERVICE_NAME", "metastore"))
                .withNetwork(network)
                .withExposePorts(Set.of(Hive4Metastore.HIVE_METASTORE_PORT))
                .withFilesToMount(hiveFilesToMount);
        this.hiveMetastore = closer.register(metastorebuilder.build());

        // Run hive server connecting to remote(above) metastore https://github.com/apache/hive/blob/a1420ed816c315d98be7ebf05cdc3ba139a68643/packaging/src/docker/README.md?plain=1#L139-L143
        Hive4HiveServer.Builder hiveHadoopBuilder = Hive4HiveServer.builder()
                .withImage(hiveImage)
                .withEnvVars(Map.of(
                        "SERVICE_NAME", "hiveserver2",
                        "HIVE_SERVER2_THRIFT_PORT", String.valueOf(HIVE_SERVER_PORT),
                        "SERVICE_OPTS", "-Xmx1G -Dhive.metastore.uris=%s".formatted(hiveMetastore.getInternalHiveMetastoreEndpoint()),
                        "IS_RESUME", "true",
                        "AWS_ACCESS_KEY_ID", MINIO_ACCESS_KEY,
                        "AWS_SECRET_KEY", MINIO_SECRET_KEY))
                .withNetwork(network)
                .withExposePorts(Set.of(HIVE_SERVER_PORT))
                .withFilesToMount(hiveFilesToMount);
        this.hiveServer = closer.register(hiveHadoopBuilder.build());
    }

    @Override
    public void start()
    {
        super.start();
        hiveMetastore.start();
        hiveServer.start();
        state = STARTED;
    }

    @Override
    public String runOnHive(String sql)
    {
        return hiveServer.runOnHive(sql);
    }

    @Override
    public HiveHadoop getHiveHadoop()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public URI getHiveMetastoreEndpoint()
    {
        return hiveMetastore.getHiveMetastoreEndpoint();
    }
}
