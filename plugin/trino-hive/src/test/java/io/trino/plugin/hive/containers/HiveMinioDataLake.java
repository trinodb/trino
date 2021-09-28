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

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.google.common.collect.ImmutableMap;
import io.trino.util.AutoCloseableCloser;
import org.testcontainers.containers.Network;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;
import static org.testcontainers.containers.Network.newNetwork;

public class HiveMinioDataLake
        implements AutoCloseable
{
    public static final String ACCESS_KEY = "accesskey";
    public static final String SECRET_KEY = "secretkey";

    private final String bucketName;
    private final Minio minio;
    private final HiveHadoop hiveHadoop;

    private final AtomicBoolean isStarted = new AtomicBoolean(false);
    private final AutoCloseableCloser closer = AutoCloseableCloser.create();

    public HiveMinioDataLake(String bucketName, Map<String, String> hiveHadoopFilesToMount)
    {
        this(bucketName, hiveHadoopFilesToMount, HiveHadoop.DEFAULT_IMAGE);
    }

    public HiveMinioDataLake(String bucketName, Map<String, String> hiveHadoopFilesToMount, String hiveHadoopImage)
    {
        this.bucketName = requireNonNull(bucketName, "bucketName is null");
        Network network = closer.register(newNetwork());
        this.minio = closer.register(
                Minio.builder()
                        .withNetwork(network)
                        .withEnvVars(ImmutableMap.<String, String>builder()
                                .put("MINIO_ACCESS_KEY", ACCESS_KEY)
                                .put("MINIO_SECRET_KEY", SECRET_KEY)
                                .build())
                        .build());
        this.hiveHadoop = closer.register(
                HiveHadoop.builder()
                        .withFilesToMount(ImmutableMap.<String, String>builder()
                                .put("hive_s3_insert_overwrite/hive-core-site.xml", "/etc/hadoop/conf/core-site.xml")
                                .putAll(hiveHadoopFilesToMount)
                                .build())
                        .withImage(hiveHadoopImage)
                        .withNetwork(network)
                        .build());
    }

    public void start()
    {
        checkState(!isStarted(), "Already started");
        try {
            this.minio.start();
            this.hiveHadoop.start();
            AmazonS3 s3Client = AmazonS3ClientBuilder
                    .standard()
                    .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(
                            "http://localhost:" + minio.getMinioApiEndpoint().getPort(),
                            "us-east-1"))
                    .withPathStyleAccessEnabled(true)
                    .withCredentials(new AWSStaticCredentialsProvider(
                            new BasicAWSCredentials(ACCESS_KEY, SECRET_KEY)))
                    .build();
            s3Client.createBucket(this.bucketName);
        }
        finally {
            isStarted.set(true);
        }
    }

    public boolean isStarted()
    {
        return isStarted.get();
    }

    public void stop()
    {
        if (!isStarted()) {
            return;
        }
        try {
            closer.close();
        }
        catch (Exception e) {
            throw new RuntimeException("Failed to stop HiveMinioDataLake", e);
        }
        finally {
            isStarted.set(false);
        }
    }

    public Minio getMinio()
    {
        return minio;
    }

    public HiveHadoop getHiveHadoop()
    {
        return hiveHadoop;
    }

    @Override
    public void close()
            throws IOException
    {
        stop();
    }
}
