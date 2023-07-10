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
import io.trino.testing.ResourcePresence;
import io.trino.testing.containers.Minio;
import io.trino.testing.minio.MinioClient;
import io.trino.util.AutoCloseableCloser;
import org.testcontainers.containers.Network;

import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkState;
import static io.trino.testing.containers.Minio.MINIO_ACCESS_KEY;
import static io.trino.testing.containers.Minio.MINIO_SECRET_KEY;
import static io.trino.testing.containers.TestContainers.getPathFromClassPathResource;
import static java.util.Objects.requireNonNull;
import static org.testcontainers.containers.Network.newNetwork;

public class HiveMinioDataLake
        implements AutoCloseable
{
    /**
     * In S3 this region is implicitly the default one. In Minio, however,
     * if we set an empty region, it will accept any.
     * So setting it by default to `us-east-1` simulates S3 better
     */
    public static final String MINIO_DEFAULT_REGION = "us-east-1";

    private final String bucketName;
    private final Minio minio;
    private final HiveHadoop hiveHadoop;

    private final AutoCloseableCloser closer = AutoCloseableCloser.create();

    private State state = State.INITIAL;
    private MinioClient minioClient;

    public HiveMinioDataLake(String bucketName)
    {
        this(bucketName, HiveHadoop.DEFAULT_IMAGE);
    }

    public HiveMinioDataLake(String bucketName, String hiveHadoopImage)
    {
        this(bucketName, ImmutableMap.of("/etc/hadoop/conf/core-site.xml", getPathFromClassPathResource("hive_minio_datalake/hive-core-site.xml")), hiveHadoopImage);
    }

    public HiveMinioDataLake(String bucketName, Map<String, String> hiveHadoopFilesToMount, String hiveHadoopImage)
    {
        this.bucketName = requireNonNull(bucketName, "bucketName is null");
        Network network = closer.register(newNetwork());
        this.minio = closer.register(
                Minio.builder()
                        .withNetwork(network)
                        .withEnvVars(ImmutableMap.<String, String>builder()
                                .put("MINIO_ACCESS_KEY", MINIO_ACCESS_KEY)
                                .put("MINIO_SECRET_KEY", MINIO_SECRET_KEY)
                                .put("MINIO_REGION", MINIO_DEFAULT_REGION)
                                .buildOrThrow())
                        .build());

        HiveHadoop.Builder hiveHadoopBuilder = HiveHadoop.builder()
                .withImage(hiveHadoopImage)
                .withNetwork(network)
                .withFilesToMount(hiveHadoopFilesToMount);
        this.hiveHadoop = closer.register(hiveHadoopBuilder.build());
    }

    public void start()
    {
        checkState(state == State.INITIAL, "Already started: %s", state);
        state = State.STARTING;
        minio.start();
        hiveHadoop.start();
        minioClient = closer.register(minio.createMinioClient());
        minio.createBucket(bucketName);
        state = State.STARTED;
    }

    public void stop()
            throws Exception
    {
        closer.close();
        state = State.STOPPED;
    }

    @ResourcePresence
    public boolean isNotStopped()
    {
        return state != State.STOPPED;
    }

    public MinioClient getMinioClient()
    {
        checkState(state == State.STARTED, "Can't provide client when MinIO state is: %s", state);
        return minioClient;
    }

    public void copyResources(String resourcePath, String target)
    {
        minio.copyResources(resourcePath, bucketName, target);
    }

    public void writeFile(byte[] contents, String target)
    {
        minio.writeFile(contents, bucketName, target);
    }

    public List<String> listFiles(String targetDirectory)
    {
        return getMinioClient().listObjects(getBucketName(), targetDirectory);
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
