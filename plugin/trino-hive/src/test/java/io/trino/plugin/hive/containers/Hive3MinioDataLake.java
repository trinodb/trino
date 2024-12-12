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

import static io.trino.plugin.hive.containers.HiveMinioDataLake.State.STARTED;
import static io.trino.testing.containers.TestContainers.getPathFromClassPathResource;

public class Hive3MinioDataLake
        extends HiveMinioDataLake
{
    private final HiveHadoop hiveHadoop;

    public Hive3MinioDataLake(String bucketName)
    {
        this(bucketName, HiveHadoop.HIVE3_IMAGE);
    }

    public Hive3MinioDataLake(String bucketName, String hiveHadoopImage)
    {
        this(bucketName, ImmutableMap.of("/etc/hadoop/conf/core-site.xml", getPathFromClassPathResource("hive_minio_datalake/hive-core-site.xml")), hiveHadoopImage);
    }

    public Hive3MinioDataLake(String bucketName, Map<String, String> hiveHadoopFilesToMount, String hiveHadoopImage)
    {
        super(bucketName);
        HiveHadoop.Builder hiveHadoopBuilder = HiveHadoop.builder()
                .withImage(hiveHadoopImage)
                .withNetwork(network)
                .withFilesToMount(hiveHadoopFilesToMount);
        this.hiveHadoop = closer.register(hiveHadoopBuilder.build());
    }

    @Override
    public void start()
    {
        super.start();
        hiveHadoop.start();
        state = STARTED;
    }

    @Override
    public String runOnHive(String sql)
    {
        return hiveHadoop.runOnHive(sql);
    }

    @Override
    public HiveHadoop getHiveHadoop()
    {
        return hiveHadoop;
    }

    @Override
    public URI getHiveMetastoreEndpoint()
    {
        return hiveHadoop.getHiveMetastoreEndpoint();
    }
}
