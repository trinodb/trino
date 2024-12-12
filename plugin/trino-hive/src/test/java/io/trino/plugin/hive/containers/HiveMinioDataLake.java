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

import java.net.URI;
import java.util.List;

import static com.google.common.base.Preconditions.checkState;
import static io.trino.plugin.hive.containers.HiveMinioDataLake.State.INITIAL;
import static io.trino.plugin.hive.containers.HiveMinioDataLake.State.STARTED;
import static io.trino.plugin.hive.containers.HiveMinioDataLake.State.STARTING;
import static io.trino.plugin.hive.containers.HiveMinioDataLake.State.STOPPED;
import static io.trino.testing.containers.Minio.MINIO_ACCESS_KEY;
import static io.trino.testing.containers.Minio.MINIO_SECRET_KEY;
import static java.util.Objects.requireNonNull;
import static org.testcontainers.containers.Network.newNetwork;

public abstract class HiveMinioDataLake
        implements AutoCloseable
{
    private static final String MINIO_DEFAULT_REGION = "us-east-1";

    private final String bucketName;
    private final Minio minio;
    private MinioClient minioClient;

    protected final AutoCloseableCloser closer = AutoCloseableCloser.create();
    protected final Network network;
    protected State state = INITIAL;

    public HiveMinioDataLake(String bucketName)
    {
        this.bucketName = requireNonNull(bucketName, "bucketName is null");
        this.network = closer.register(newNetwork());
        this.minio = closer.register(
                Minio.builder()
                        .withNetwork(network)
                        .withEnvVars(ImmutableMap.<String, String>builder()
                                .put("MINIO_ACCESS_KEY", MINIO_ACCESS_KEY)
                                .put("MINIO_SECRET_KEY", MINIO_SECRET_KEY)
                                .put("MINIO_REGION", MINIO_DEFAULT_REGION)
                                .buildOrThrow())
                        .build());
    }

    public void start()
    {
        checkState(state == INITIAL, "Already started: %s", state);
        state = STARTING;
        minio.start();
        minioClient = closer.register(minio.createMinioClient());
        minio.createBucket(bucketName);
    }

    public void stop()
            throws Exception
    {
        closer.close();
        state = STOPPED;
    }

    public Network getNetwork()
    {
        return network;
    }

    public MinioClient getMinioClient()
    {
        checkState(state == STARTED, "Can't provide client when MinIO state is: %s", state);
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

    public abstract String runOnHive(String sql);

    public abstract HiveHadoop getHiveHadoop();

    public abstract URI getHiveMetastoreEndpoint();

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

    protected enum State
    {
        INITIAL,
        STARTING,
        STARTED,
        STOPPED,
    }
}
