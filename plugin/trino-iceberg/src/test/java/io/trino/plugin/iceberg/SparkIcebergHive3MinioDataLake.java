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
package io.trino.plugin.iceberg;

import com.google.common.collect.ImmutableMap;
import io.trino.plugin.base.util.AutoCloseableCloser;
import io.trino.plugin.hive.containers.Hive3MinioDataLake;
import io.trino.plugin.hive.containers.HiveHadoop;
import io.trino.testing.containers.Minio;

import static io.trino.testing.containers.TestContainers.getPathFromClassPathResource;

public final class SparkIcebergHive3MinioDataLake
        implements AutoCloseable
{
    private final AutoCloseableCloser closer = AutoCloseableCloser.create();
    private final Hive3MinioDataLake hiveMinio;
    private final SparkIceberg spark;

    public SparkIcebergHive3MinioDataLake(String bucketName)
    {
        hiveMinio = closer.register(new Hive3MinioDataLake(bucketName));
        hiveMinio.start();

        SparkIceberg.Builder sparkIcebergBuilder = SparkIceberg.builder()
                .withNetwork(hiveMinio.getNetwork())
                .withFilesToMount(ImmutableMap.of(
                        "/spark/conf/spark-defaults.conf", getPathFromClassPathResource("spark/spark-defaults.conf"),
                        "/spark/conf/log4j2.properties", getPathFromClassPathResource("spark/log4j2.properties")));
        spark = closer.register(sparkIcebergBuilder.build());
        spark.start();
    }

    public HiveHadoop hiveHadoop()
    {
        return hiveMinio.getHiveHadoop();
    }

    public Minio minio()
    {
        return hiveMinio.getMinio();
    }

    @Override
    public void close()
            throws Exception
    {
        closer.close();
    }
}
