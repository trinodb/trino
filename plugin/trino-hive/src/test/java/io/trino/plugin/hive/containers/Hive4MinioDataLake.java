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
import io.trino.plugin.base.util.AutoCloseableCloser;
import io.trino.testing.containers.Minio;
import io.trino.testing.minio.MinioClient;
import org.testcontainers.containers.Network;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.Preconditions.checkState;
import static io.trino.plugin.hive.containers.Hive4HiveServer.HIVE_SERVER_PORT;
import static io.trino.testing.containers.Minio.MINIO_ACCESS_KEY;
import static io.trino.testing.containers.Minio.MINIO_SECRET_KEY;
import static io.trino.testing.containers.TestContainers.getPathFromClassPathResource;
import static java.util.Objects.requireNonNull;
import static org.testcontainers.containers.Network.newNetwork;

public class Hive4MinioDataLake
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
    private final Hive4HiveServer hiveServer;
    private final Hive4Metastore hiveMetastore;
    private final AutoCloseableCloser closer = AutoCloseableCloser.create();

    private final Network network;

    private State state = State.INITIAL;
    private MinioClient minioClient;

    public Hive4MinioDataLake(String bucketName)
    {
        this(bucketName, Hive4Metastore.HIVE4_IMAGE);
    }

    public Hive4MinioDataLake(String bucketName, String hiveImage)
    {
        this(bucketName, ImmutableMap.of("/opt/hive/conf/hive-site.xml", getPathFromClassPathResource("hive_minio_datalake/hive4-hive-site.xml")), hiveImage);
    }

    public Hive4MinioDataLake(String bucketName, Map<String, String> hiveFilesToMount, String hiveImage)
    {
        this.bucketName = requireNonNull(bucketName, "bucketName is null");
        network = closer.register(newNetwork());
        this.minio = closer.register(
                Minio.builder()
                        .withNetwork(network)
                        .withEnvVars(ImmutableMap.<String, String>builder()
                                .put("MINIO_ACCESS_KEY", MINIO_ACCESS_KEY)
                                .put("MINIO_SECRET_KEY", MINIO_SECRET_KEY)
                                .put("MINIO_REGION", MINIO_DEFAULT_REGION)
                                .buildOrThrow())
                        .build());

        Hive4Metastore.Builder metastorebuilder = Hive4Metastore.builder()
                .withImage(hiveImage)
                .withEnvVars(Map.of("SERVICE_NAME", "metastore"))
                .withNetwork(network)
                .withExposePorts(Set.of(Hive4Metastore.HIVE_METASTORE_PORT))
                .withFilesToMount(hiveFilesToMount);
        this.hiveMetastore = closer.register(metastorebuilder.build());

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

    public void start()
    {
        checkState(state == State.INITIAL, "Already started: %s", state);
        state = State.STARTING;
        minio.start();
        hiveMetastore.start();
        hiveServer.start();
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

    public Network getNetwork()
    {
        return network;
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

    public List<String> listFiles(String targetDirectory)
    {
        return getMinioClient().listObjects(getBucketName(), targetDirectory);
    }

    public Minio getMinio()
    {
        return minio;
    }

    public Hive4HiveServer getHiveServer()
    {
        return hiveServer;
    }

    public Hive4Metastore getHiveMetastore()
    {
        return hiveMetastore;
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
