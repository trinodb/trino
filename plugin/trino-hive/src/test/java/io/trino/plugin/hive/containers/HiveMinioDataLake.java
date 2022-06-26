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
import com.google.common.reflect.ClassPath;
import io.trino.testing.containers.Minio;
import io.trino.testing.minio.MinioClient;
import io.trino.util.AutoCloseableCloser;
import net.jodah.failsafe.Failsafe;
import net.jodah.failsafe.RetryPolicy;
import org.testcontainers.containers.Network;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkState;
import static io.trino.testing.containers.TestContainers.getPathFromClassPathResource;
import static java.time.temporal.ChronoUnit.MINUTES;
import static java.time.temporal.ChronoUnit.SECONDS;
import static java.util.Objects.requireNonNull;
import static java.util.regex.Matcher.quoteReplacement;
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
                                .put("MINIO_ACCESS_KEY", ACCESS_KEY)
                                .put("MINIO_SECRET_KEY", SECRET_KEY)
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
        minioClient = initMinioClient();
        state = State.STARTED;
    }

    public void stop()
            throws Exception
    {
        closer.close();
        state = State.STOPPED;
    }

    public MinioClient getMinioClient()
    {
        checkState(state == State.STARTED, "Can't provide client when MinIO state is: %s", state);
        return minioClient;
    }

    public void copyResources(String resourcePath, String target)
    {
        try {
            for (ClassPath.ResourceInfo resourceInfo : ClassPath.from(MinioClient.class.getClassLoader())
                    .getResources()) {
                if (resourceInfo.getResourceName().startsWith(resourcePath)) {
                    String fileName = resourceInfo.getResourceName().replaceFirst("^" + Pattern.quote(resourcePath), quoteReplacement(target));
                    writeFile(resourceInfo.asByteSource().read(), fileName);
                }
            }
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public void writeFile(byte[] contents, String target)
    {
        getMinioClient().putObject(getBucketName(), contents, target);
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

    public String getMinioAddress()
    {
        return "http://" + getMinio().getMinioApiEndpoint();
    }

    @Override
    public void close()
            throws Exception
    {
        stop();
    }

    private MinioClient initMinioClient()
    {
        MinioClient minioClient = new MinioClient(getMinioAddress(), ACCESS_KEY, SECRET_KEY);
        closer.register(minioClient);

        // use retry loop for minioClient.makeBucket as minio container tends to return "Server not initialized, please try again" error
        // for some time after starting up
        RetryPolicy<Object> retryPolicy = new RetryPolicy<>()
                .withMaxDuration(Duration.of(2, MINUTES))
                .withMaxAttempts(Integer.MAX_VALUE) // limited by MaxDuration
                .withDelay(Duration.of(10, SECONDS));
        Failsafe.with(retryPolicy).run(() -> minioClient.makeBucket(bucketName));

        return minioClient;
    }

    private enum State
    {
        INITIAL,
        STARTING,
        STARTED,
        STOPPED,
    }
}
