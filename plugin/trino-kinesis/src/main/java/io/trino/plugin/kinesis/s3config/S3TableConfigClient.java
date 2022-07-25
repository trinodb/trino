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

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.AmazonS3URI;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
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

import javax.annotation.PostConstruct;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
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
    private final Optional<String> bucketUrl;
    private volatile long lastCheck;
    private volatile ScheduledFuture<?> updateTaskHandle;

    private final Map<String, KinesisStreamDescription> descriptors = Collections.synchronizedMap(new HashMap<>());

    @Inject
    public S3TableConfigClient(
            KinesisConfig connectorConfig,
            KinesisClientProvider clientManager,
            JsonCodec<KinesisStreamDescription> jsonCodec)
    {
        requireNonNull(connectorConfig, "connectorConfig is null");
        this.tableDescriptionRefreshInterval = connectorConfig.getTableDescriptionRefreshInterval();
        this.clientManager = requireNonNull(clientManager, "clientManager is null");
        this.streamDescriptionCodec = requireNonNull(jsonCodec, "jsonCodec is null");

        // If using S3 start thread that periodically looks for updates
        if (connectorConfig.getTableDescriptionLocation().startsWith("s3://")) {
            this.bucketUrl = Optional.of(connectorConfig.getTableDescriptionLocation());
        }
        else {
            this.bucketUrl = Optional.empty();
        }
    }

    @PostConstruct
    protected void startS3Updates()
    {
        if (this.bucketUrl.isPresent()) {
            ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
            this.updateTaskHandle = scheduler.scheduleAtFixedRate(this::updateTablesFromS3, 5_000, tableDescriptionRefreshInterval.toMillis(), TimeUnit.MILLISECONDS);
        }
    }

    /**
     * Indicates this class is being used and actively reading table definitions from S3.
     */
    public boolean isUsingS3()
    {
        return bucketUrl.isPresent() && (bucketUrl.get().startsWith("s3://"));
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
    private List<S3ObjectSummary> getObjectSummaries()
    {
        AmazonS3Client s3client = clientManager.getS3Client();
        AmazonS3URI directoryURI = new AmazonS3URI(bucketUrl.get());

        List<S3ObjectSummary> result = new ArrayList<>();
        try {
            log.info("Getting the listing of objects in the S3 table config directory: bucket %s prefix %s :", directoryURI.getBucket(), directoryURI.getKey());
            ListObjectsRequest request = new ListObjectsRequest()
                    .withBucketName(directoryURI.getBucket())
                    .withPrefix(directoryURI.getKey() + "/")
                    .withDelimiter("/")
                    .withMaxKeys(25);
            ObjectListing response;

            do {
                response = s3client.listObjects(request);

                result.addAll(response.getObjectSummaries());
                request.setMarker(response.getNextMarker());
            }
            while (response.isTruncated());

            log.info("Completed getting S3 object listing.");
        }
        catch (AmazonClientException e) {
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

        AmazonS3Client s3client = clientManager.getS3Client();

        for (S3ObjectSummary summary : getObjectSummaries()) {
            if (!descriptors.containsKey(summary.getKey()) || summary.getLastModified().getTime() >= lastCheck) {
                // New or updated file, so we must read from AWS
                if (summary.getKey().endsWith("/")) {
                    continue;
                }

                log.info("Getting : %s - %s", summary.getBucketName(), summary.getKey());
                S3Object object = s3client.getObject(new GetObjectRequest(summary.getBucketName(), summary.getKey()));

                try (BufferedReader reader = new BufferedReader(new InputStreamReader(object.getObjectContent(), UTF_8))) {
                    KinesisStreamDescription table = streamDescriptionCodec.fromJson(CharStreams.toString(reader));
                    descriptors.put(summary.getKey(), table);
                    log.info("Put table description into the map from %s", summary.getKey());
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
