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
package io.trino.plugin.kinesis.s3config;

import com.google.common.collect.ImmutableMap;
import com.google.common.io.CharStreams;
import com.google.inject.Inject;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import io.trino.plugin.kinesis.KinesisClientProvider;
import io.trino.plugin.kinesis.KinesisConfig;
import io.trino.plugin.kinesis.KinesisStreamDescription;
import io.trino.spi.connector.SchemaTableName;
import jakarta.annotation.PostConstruct;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Uri;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsResponse;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

/**
 * Utility class to retrieve table definitions from a common place on Amazon S3.
 * <p>
 * This is so that we can add new tables in a central "metastore" location without
 * having to update every single node with the files.
 * <p>
 * This makes calls to Amazon AWS using the S3 client.
 */
public class S3TableConfigClient
        implements Runnable
{
    private static final Logger log = Logger.get(S3TableConfigClient.class);

    private final KinesisClientProvider clientManager;
    private final JsonCodec<KinesisStreamDescription> streamDescriptionCodec;
    private final Duration tableDescriptionRefreshInterval;
    private final Optional<S3Uri> bucketS3Uri;
    private volatile long lastCheck;
    private volatile ScheduledFuture<?> updateTaskHandle;

    private final Map<String, KinesisStreamDescription> descriptors = Collections.synchronizedMap(new HashMap<>());

    @Inject
    public S3TableConfigClient(
            KinesisConfig connectorConfig,
            KinesisClientProvider clientManager,
            JsonCodec<KinesisStreamDescription> jsonCodec)
    {
        this.tableDescriptionRefreshInterval = connectorConfig.getTableDescriptionRefreshInterval();
        this.clientManager = requireNonNull(clientManager, "clientManager is null");
        this.streamDescriptionCodec = requireNonNull(jsonCodec, "jsonCodec is null");

        // If using S3 start thread that periodically looks for updates
        if (connectorConfig.getTableDescriptionLocation().startsWith("s3://")) {
            String bucketURL = connectorConfig.getTableDescriptionLocation();
            this.bucketS3Uri = Optional.of(clientManager.getS3Client().utilities().parseUri(URI.create(bucketURL)));
        }
        else {
            this.bucketS3Uri = Optional.empty();
        }
    }

    @PostConstruct
    protected void startS3Updates()
    {
        if (this.bucketS3Uri.isPresent()) {
            ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
            this.updateTaskHandle = scheduler.scheduleAtFixedRate(this::updateTablesFromS3, 5_000, tableDescriptionRefreshInterval.toMillis(), TimeUnit.MILLISECONDS);
        }
    }

    /**
     * Indicates this class is being used and actively reading table definitions from S3.
     */
    public boolean isUsingS3()
    {
        return bucketS3Uri.isPresent() && bucketS3Uri.get().uri().toString().startsWith("s3://");
    }

    /**
     * Main entry point to get table definitions from S3 using bucket and object directory
     * given in the configuration.
     * <p>
     * For safety, an immutable copy built from the internal map is returned.
     */
    public Map<SchemaTableName, KinesisStreamDescription> getTablesFromS3()
    {
        updateTablesFromS3();
        Collection<KinesisStreamDescription> streamValues = this.descriptors.values();
        ImmutableMap.Builder<SchemaTableName, KinesisStreamDescription> builder = ImmutableMap.builder();
        for (KinesisStreamDescription stream : streamValues) {
            builder.put(new SchemaTableName(stream.getSchemaName(), stream.getTableName()), stream);
        }
        return builder.buildOrThrow();
    }

    @Override
    public void run()
    {
        if (isUsingS3() && updateTaskHandle != null) {
            updateTaskHandle.cancel(true);
        }
    }

    /**
     * Call S3 to get the most recent object list.
     * <p>
     * This is an object list request to AWS in the given "directory".
     */
    private List<S3Object> getS3Objects()
    {
        S3Client s3client = clientManager.getS3Client();
        S3Uri directoryURI = bucketS3Uri.get();

        List<S3Object> result = new ArrayList<>();
        try {
            log.info("Getting the listing of objects in the S3 table config directory: bucket %s prefix %s :", directoryURI.bucket().get(), directoryURI.key().get());
            ListObjectsRequest request = ListObjectsRequest.builder()
                    .bucket(directoryURI.bucket().get())
                    .prefix(directoryURI.key().get() + "/")
                    .delimiter("/")
                    .maxKeys(25).build();
            ListObjectsResponse response;

            do {
                response = s3client.listObjects(request);

                result.addAll(response.contents());
                request = request.toBuilder().marker(response.nextMarker()).build();
            }
            while (response.isTruncated());

            log.info("Completed getting S3 object listing.");
        }
        catch (SdkException e) {
            log.error("Skipping update as faced error fetching table descriptions from S3 %s", e);
        }
        return result;
    }

    /**
     * Connect to S3 directory to look for new or updated table definitions and then
     * update the map.
     */
    private void updateTablesFromS3()
    {
        long now = System.currentTimeMillis();

        S3Client s3client = clientManager.getS3Client();
        String bucket = bucketS3Uri.get().bucket().get();

        for (S3Object s3Object : getS3Objects()) {
            if (!descriptors.containsKey(s3Object.key()) || s3Object.lastModified().toEpochMilli() >= lastCheck) {
                // New or updated file, so we must read from AWS
                if (s3Object.key().endsWith("/")) {
                    continue;
                }

                log.info("Getting : %s - %s", bucket, s3Object.key());
                GetObjectRequest request = GetObjectRequest.builder()
                        .bucket(bucket)
                        .key(s3Object.key()).build();
                ResponseInputStream<GetObjectResponse> object = s3client.getObject(request);
                try (BufferedReader reader = new BufferedReader(new InputStreamReader(object, UTF_8))) {
                    KinesisStreamDescription table = streamDescriptionCodec.fromJson(CharStreams.toString(reader));
                    descriptors.put(s3Object.key(), table);
                    log.info("Put table description into the map from %s", s3Object.key());
                }
                catch (IOException iox) {
                    log.error(iox, "Problem reading input stream from object.");
                    throw new RuntimeException(iox);
                }
            }
        }

        log.info("Completed updating table definitions from S3.");
        lastCheck = now;
    }
}
