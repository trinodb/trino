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

import io.trino.plugin.base.util.AutoCloseableCloser;
import io.trino.plugin.hive.containers.HiveHadoop;
import io.trino.plugin.hive.containers.HiveMinioDataLake;
import io.trino.testing.containers.Minio;
import org.testcontainers.containers.GenericContainer;

import static io.trino.testing.TestingProperties.getDockerImagesVersion;
import static org.testcontainers.utility.MountableFile.forClasspathResource;

public final class SparkDeltaLake
        implements AutoCloseable
{
    private final AutoCloseableCloser closer = AutoCloseableCloser.create();
    private final HiveMinioDataLake hiveMinio;

    public SparkDeltaLake(String bucketName)
    {
        hiveMinio = closer.register(new HiveMinioDataLake(bucketName));
        hiveMinio.start();

        closer.register(new GenericContainer<>("ghcr.io/trinodb/testing/spark3-delta:" + getDockerImagesVersion()))
                .withCopyFileToContainer(forClasspathResource("spark-defaults.conf"), "/spark/conf/spark-defaults.conf")
                .withNetwork(hiveMinio.getNetwork())
                .start();
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
