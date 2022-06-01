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
package io.trino.plugin.deltalake.util;

import io.trino.testing.minio.MinioClient;
import net.jodah.failsafe.Failsafe;
import net.jodah.failsafe.RetryPolicy;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.plugin.deltalake.util.MinioContainer.MINIO_ACCESS_KEY;
import static io.trino.plugin.deltalake.util.MinioContainer.MINIO_SECRET_KEY;
import static java.time.temporal.ChronoUnit.MINUTES;
import static java.time.temporal.ChronoUnit.SECONDS;

public final class DockerizedMinioDataLake
        extends DockerizedDataLake
{
    private final MinioContainer minio;
    private final String bucketName;
    private final MinioClient minioClient;

    public DockerizedMinioDataLake(
            String bucketName,
            Optional<String> hadoopBaseImage,
            Map<String, String> hadoopImageResourceMap,
            Map<String, String> hadoopImageFileMap)
    {
        super(hadoopBaseImage, hadoopImageResourceMap, hadoopImageFileMap);
        try {
            this.bucketName = bucketName;
            this.minio = initMinioContainer();
            this.minioClient = initMinioClient(this.minio);
        }
        catch (Exception e) {
            try {
                closer.close();
            }
            catch (IOException closeException) {
                if (e != closeException) {
                    e.addSuppressed(closeException);
                }
            }
            throw e;
        }
    }

    public DockerizedMinioDataLake(
            String bucketName,
            TestingHadoop testingHadoop)
    {
        super(testingHadoop);
        try {
            this.bucketName = bucketName;
            this.minio = initMinioContainer();
            this.minioClient = initMinioClient(this.minio);
        }
        catch (Exception e) {
            try {
                closer.close();
            }
            catch (IOException closeException) {
                if (e != closeException) {
                    e.addSuppressed(closeException);
                }
            }
            throw e;
        }
    }

    private MinioContainer initMinioContainer()
    {
        MinioContainer container = new MinioContainer(network);
        closer.register(container);
        container.start();
        return container;
    }

    private MinioClient initMinioClient(MinioContainer container)
    {
        MinioClient minioClient = new MinioClient(container.getMinioAddress(), MINIO_ACCESS_KEY, MINIO_SECRET_KEY);
        closer.register(minioClient::close);

        // use retry loop for minioClient.makeBucket as minio container tends to return "Server not initialized, please try again" error
        // for some time after starting up
        RetryPolicy<Object> retryPolicy = new RetryPolicy<>()
                .withMaxDuration(Duration.of(2, MINUTES))
                .withMaxAttempts(Integer.MAX_VALUE) // limited by MaxDuration
                .withDelay(Duration.of(10, SECONDS));
        Failsafe.with(retryPolicy).run(() -> minioClient.makeBucket(bucketName));

        return minioClient;
    }

    public void copyResources(String resourcePath, String target)
    {
        minioClient.copyResourcePath(bucketName, resourcePath, target);
    }

    public void writeFile(byte[] contents, String target)
    {
        minioClient.putObject(bucketName, contents, target);
    }

    public List<String> listFiles(String targetDirectory)
    {
        return minioClient.listObjects(bucketName, targetDirectory);
    }

    public String getMinioAddress()
    {
        return minio.getMinioAddress();
    }
}
