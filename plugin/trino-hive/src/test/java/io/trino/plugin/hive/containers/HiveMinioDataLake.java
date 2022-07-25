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
import io.trino.testing.containers.Minio;
import io.trino.util.AutoCloseableCloser;
import org.testcontainers.containers.Network;

import java.util.Map;

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

    private final AutoCloseableCloser closer = AutoCloseableCloser.create();

    private State state = State.INITIAL;
    private AmazonS3 s3Client;

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
                                .buildOrThrow())
                        .build());
        this.hiveHadoop = closer.register(
                HiveHadoop.builder()
                        .withFilesToMount(ImmutableMap.<String, String>builder()
                                .put("hive_minio_datalake/hive-core-site.xml", "/etc/hadoop/conf/core-site.xml")
                                .putAll(hiveHadoopFilesToMount)
                                .buildOrThrow())
                        .withImage(hiveHadoopImage)
                        .withNetwork(network)
                        .build());
    }

    public void start()
    {
        checkState(state == State.INITIAL, "Already started: %s", state);
        state = State.STARTING;
        minio.start();
        hiveHadoop.start();
        s3Client = AmazonS3ClientBuilder
                .standard()
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(
                        "http://localhost:" + minio.getMinioApiEndpoint().getPort(),
                        "us-east-1"))
                .withPathStyleAccessEnabled(true)
                .withCredentials(new AWSStaticCredentialsProvider(
                        new BasicAWSCredentials(ACCESS_KEY, SECRET_KEY)))
                .build();
        s3Client.createBucket(this.bucketName);
        closer.register(() -> s3Client.shutdown());
        state = State.STARTED;
    }

    public AmazonS3 getS3Client()
    {
        checkState(state == State.STARTED, "Can't provide client when MinIO state is: %s", state);
        return s3Client;
    }

    public void stop()
            throws Exception
    {
        closer.close();
        state = State.STOPPED;
    }

    public Minio getMinio()
    {
        return minio;
    }

    public HiveHadoop getHiveHadoop()
    {
        return hiveHadoop;
    }

    public String getBucketName()
    {
        return bucketName;
    }

    @Override
    public void close()
            throws Exception
    {
        stop();
    }

    private enum State
    {
        INITIAL,
        STARTING,
        STARTED,
        STOPPED,
    }
}
