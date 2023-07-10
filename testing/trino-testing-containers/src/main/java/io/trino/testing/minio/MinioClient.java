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
package io.trino.testing.minio;

import com.google.common.collect.Sets;
import com.google.common.io.ByteSource;
import com.google.common.reflect.ClassPath;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import io.airlift.log.Logger;
import io.minio.BucketExistsArgs;
import io.minio.CloseableIterator;
import io.minio.CopyObjectArgs;
import io.minio.CopySource;
import io.minio.ListObjectsArgs;
import io.minio.ListenBucketNotificationArgs;
import io.minio.MakeBucketArgs;
import io.minio.PutObjectArgs;
import io.minio.RemoveObjectArgs;
import io.minio.Result;
import io.minio.messages.Event;
import io.minio.messages.NotificationRecords;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.function.Consumer;
import java.util.regex.Pattern;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Streams.stream;
import static com.google.common.util.concurrent.Futures.addCallback;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.minio.messages.EventType.OBJECT_ACCESSED_ANY;
import static io.minio.messages.EventType.OBJECT_CREATED_ANY;
import static io.minio.messages.EventType.OBJECT_REMOVED_ANY;
import static java.util.Objects.requireNonNull;
import static java.util.regex.Matcher.quoteReplacement;

public class MinioClient
        implements AutoCloseable
{
    private final Logger logger = Logger.get(MinioClient.class);

    public static final String DEFAULT_MINIO_ENDPOINT = "http://minio:9080";
    public static final String DEFAULT_MINIO_ACCESS_KEY = "minio-access-key";
    public static final String DEFAULT_MINIO_SECRET_KEY = "minio-secret-key";

    private static Set<String> createdBuckets = Sets.newConcurrentHashSet();

    private final io.minio.MinioClient client;
    private final ListeningExecutorService executor = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(32));

    private static final String[] ALL_MINIO_EVENTS = new String[] {
            OBJECT_CREATED_ANY.toString(),
            OBJECT_REMOVED_ANY.toString(),
            OBJECT_ACCESSED_ANY.toString()
    };

    public MinioClient()
    {
        this(DEFAULT_MINIO_ENDPOINT, DEFAULT_MINIO_ACCESS_KEY, DEFAULT_MINIO_SECRET_KEY);
    }

    public MinioClient(String endpoint, String accessKey, String secretKey)
    {
        client = io.minio.MinioClient.builder()
                .endpoint(endpoint)
                .credentials(accessKey, secretKey)
                .build();
    }

    public void copyResourcePath(String bucket, String resourcePath, String target)
    {
        ensureBucketExists(bucket);

        try {
            ClassPath.from(MinioClient.class.getClassLoader())
                    .getResources().stream()
                    .filter(resourceInfo -> resourceInfo.getResourceName().startsWith(resourcePath))
                    .forEach(resourceInfo -> {
                        String fileName = resourceInfo.getResourceName().replaceFirst("^" + Pattern.quote(resourcePath), quoteReplacement(target));
                        putObject(bucket, resourceInfo.asByteSource(), fileName);
                    });
        }
        catch (IOException e) {
            logger.warn(e, "Could not copy resources from classpath");
            throw new UncheckedIOException(e);
        }
    }

    public void putObject(String bucket, byte[] contents, String targetPath)
    {
        ensureBucketExists(bucket);

        putObject(bucket, ByteSource.wrap(contents), targetPath);
    }

    public void captureBucketNotifications(String bucket, Consumer<Event> consumer)
    {
        ensureBucketExists(bucket);

        ListenableFuture<?> future = executor.submit(new NotificationListener(client, bucket, consumer));

        addCallback(future, new FutureCallback<Object>()
        {
            @Override
            public void onSuccess(Object result)
            {
                logger.info("Completed notification listener for bucket %s", bucket);
            }

            @Override
            public void onFailure(Throwable t)
            {
                logger.warn(t, "Notification listener for bucket %s threw exception", bucket);
            }
        }, directExecutor());
    }

    public List<String> listObjects(String bucket, String path)
    {
        try {
            return stream(client.listObjects(
                    ListObjectsArgs.builder()
                            .bucket(bucket)
                            .prefix(path)
                            .recursive(true)
                            .useUrlEncodingType(false)
                            .build()))
                    .map(result -> {
                        try {
                            return result.get().objectName();
                        }
                        catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    }).collect(toImmutableList());
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void makeBucket(String bucketName)
    {
        if (!createdBuckets.add(bucketName)) {
            // Forbid to create a bucket with given name more than once per class loader.
            // The reason for that is that bucket name is used as a key in TrinoFileSystemCache which is
            // managed in static manner. Allowing creating same bucket twice (even for two different minio environments)
            // leads to hard to understand problems when one test influences the other via unexpected cache lookups.
            throw new IllegalArgumentException("Bucket " + bucketName + " already created in this classloader");
        }
        try {
            client.makeBucket(
                    MakeBucketArgs.builder()
                            .bucket(bucketName)
                            .build());
        }
        catch (Exception e) {
            // revert bucket registration so we can retry the call on transient errors
            createdBuckets.remove(bucketName);
            throw new RuntimeException(e);
        }
    }

    public void ensureBucketExists(String bucketName)
    {
        try {
            if (!client.bucketExists(BucketExistsArgs.builder()
                    .bucket(bucketName)
                    .build())) {
                makeBucket(bucketName);
            }
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void putObject(String bucket, ByteSource byteSource, String targetPath)
    {
        try {
            try (InputStream inputStream = byteSource.openStream()) {
                client.putObject(
                        PutObjectArgs.builder()
                                .bucket(bucket)
                                .object(targetPath)
                                .stream(inputStream, byteSource.size(), -1)
                                .build());
            }
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void copyObject(String sourceBucket, String sourceKey, String targetBucket, String targetKey)
    {
        try {
            client.copyObject(CopyObjectArgs.builder()
                    .source(CopySource.builder()
                            .bucket(sourceBucket)
                            .object(sourceKey)
                            .build())
                    .bucket(targetBucket)
                    .object(targetKey)
                    .build());
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void removeObject(String bucket, String key)
    {
        try {
            client.removeObject(RemoveObjectArgs.builder()
                    .bucket(bucket)
                    .object(key)
                    .build());
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close()
    {
        executor.shutdownNow();
    }

    private static class NotificationListener
            implements Runnable
    {
        private final io.minio.MinioClient client;
        private final String bucket;
        private final Consumer<Event> consumer;

        private NotificationListener(io.minio.MinioClient client, String bucket, Consumer<Event> consumer)
        {
            this.client = requireNonNull(client, "client is null");
            this.bucket = requireNonNull(bucket, "bucket is null");
            this.consumer = requireNonNull(consumer, "consumer is null");
        }

        @Override
        public void run()
        {
            try (CloseableIterator<Result<NotificationRecords>> iterator = client.listenBucketNotification(
                    ListenBucketNotificationArgs.builder()
                            .bucket(bucket)
                            .prefix("*")
                            .suffix("*")
                            .events(ALL_MINIO_EVENTS)
                            .build())) {
                while (iterator.hasNext()) {
                    NotificationRecords records = iterator.next().get();
                    records.events().forEach(consumer);
                }
            }
            catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }
}
