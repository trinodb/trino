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
package com.qubole.presto.kinesis.s3config;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.AmazonS3URI;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.facebook.presto.spi.SchemaTableName;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import com.qubole.presto.kinesis.ConnectorShutdown;
import com.qubole.presto.kinesis.KinesisClientProvider;
import com.qubole.presto.kinesis.KinesisConnectorConfig;
import com.qubole.presto.kinesis.KinesisStreamDescription;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

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
        implements ConnectorShutdown
{
    private static final Logger log = Logger.get(S3TableConfigClient.class);

    public final KinesisConnectorConfig kinesisConnectorConfig;
    private final KinesisClientProvider clientManager;
    private final JsonCodec<KinesisStreamDescription> streamDescriptionCodec;

    private final String bucketUrl;
    private long lastCheck;
    private ScheduledFuture<?> updateTaskHandle;

    private Map<String, KinesisStreamDescription> internalMap =
            Collections.synchronizedMap(new HashMap<String, KinesisStreamDescription>());

    @Inject
    public S3TableConfigClient(KinesisConnectorConfig aConnectorConfig,
            KinesisClientProvider aClientManager,
            JsonCodec<KinesisStreamDescription> jsonCodec)
    {
        this.kinesisConnectorConfig = requireNonNull(aConnectorConfig, "connector configuration object is null");
        this.clientManager = requireNonNull(aClientManager, "client manager object is null");
        this.streamDescriptionCodec = requireNonNull(jsonCodec, "JSON codec object is null");

        // If using S3 start thread that periodically looks for updates
        this.bucketUrl = this.kinesisConnectorConfig.getTableDescriptionsS3();
        if (!this.bucketUrl.isEmpty()) {
            startS3Updates();
        }
    }

    /**
     * Indicates this class is being used and actively reading table definitions from S3.
     */
    public boolean isUsingS3()
    {
        return !this.bucketUrl.isEmpty();
    }

    /**
     * Main entry point to get table definitions from S3 using bucket and object directory
     * given in the configuration.
     * <p>
     * For safety, an immutable copy built from the internal map is returned.
     *
     * @return
     */
    public Map<SchemaTableName, KinesisStreamDescription> getTablesFromS3()
    {
        Collection<KinesisStreamDescription> streamValues = this.internalMap.values();
        ImmutableMap.Builder<SchemaTableName, KinesisStreamDescription> builder = ImmutableMap.builder();
        for (KinesisStreamDescription stream : streamValues) {
            builder.put(new SchemaTableName(stream.getSchemaName(), stream.getTableName()), stream);
        }
        return builder.build();
    }

    /**
     * Shutdown any periodic update jobs.
     */
    @Override
    public void shutdown()
    {
        if (isUsingS3() && updateTaskHandle != null) {
            updateTaskHandle.cancel(true);
        }
        return;
    }

    protected void startS3Updates()
    {
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        this.updateTaskHandle =
                scheduler.scheduleAtFixedRate(() -> updateTablesFromS3(), 5, 600, TimeUnit.SECONDS);
        return;
    }

    /**
     * Call S3 to get the most recent object list.
     * <p>
     * This is an object list request to AWS in the given "directory".
     *
     * @return
     */
    protected List<S3ObjectSummary> getObjectSummaries()
    {
        AmazonS3Client s3client = this.clientManager.getS3Client();
        AmazonS3URI directoryURI = new AmazonS3URI(this.bucketUrl);

        ArrayList<S3ObjectSummary> returnList = new ArrayList<S3ObjectSummary>();
        try {
            log.info("Getting the listing of objects in the S3 table config directory: bucket %s prefix %s :", directoryURI.getBucket(), directoryURI.getKey());
            ListObjectsRequest req = new ListObjectsRequest().withBucketName(directoryURI.getBucket())
                    .withPrefix(directoryURI.getKey() + "/").withDelimiter("/").withMaxKeys(25);
            ObjectListing result;

            do {
                result = s3client.listObjects(req);

                returnList.addAll(result.getObjectSummaries());
                req.setMarker(result.getNextMarker());
            }
            while (result.isTruncated());

            log.info("Completed getting S3 object listing.");
        }
        catch (AmazonServiceException ase) {
            StringBuilder sb = new StringBuilder();
            sb.append("Caught an AmazonServiceException, which means your request made it ");
            sb.append("to Amazon S3, but was rejected with an error response for some reason.\n");
            sb.append("Error Message:    " + ase.getMessage());
            sb.append("HTTP Status Code: " + ase.getStatusCode());
            sb.append("AWS Error Code:   " + ase.getErrorCode());
            sb.append("Error Type:       " + ase.getErrorType());
            sb.append("Request ID:       " + ase.getRequestId());
            log.error(sb.toString(), ase);
        }
        catch (AmazonClientException ace) {
            StringBuilder sb = new StringBuilder();
            sb.append("Caught an AmazonClientException, " +
                    "which means the client encountered " +
                    "an internal error while trying to communicate" +
                    " with S3, " +
                    "such as not being able to access the network.");
            sb.append("Error Message: " + ace.getMessage());
            log.error(sb.toString(), ace);
        }

        return returnList;
    }

    /**
     * Connect to S3 directory to look for new or updated table definitions and then
     * update the map.
     */
    protected void updateTablesFromS3()
    {
        long now = System.currentTimeMillis();

        List<S3ObjectSummary> objectList = this.getObjectSummaries();
        AmazonS3Client s3client = this.clientManager.getS3Client();
        AmazonS3URI directoryURI = new AmazonS3URI(this.bucketUrl);

        for (S3ObjectSummary objInfo : objectList) {
            if (!this.internalMap.containsKey(objInfo.getKey()) || objInfo.getLastModified().getTime() >= this.lastCheck) {
                // New or updated file, so we must read from AWS
                try {
                    if (objInfo.getKey().endsWith("/")) {
                        continue;
                    }

                    log.info("Getting : %s - %s", objInfo.getBucketName(), objInfo.getKey());
                    S3Object object = s3client.getObject(
                            new GetObjectRequest(objInfo.getBucketName(), objInfo.getKey()));

                    StringBuilder resultStr = new StringBuilder("");
                    try (BufferedReader reader = new BufferedReader(new InputStreamReader(object.getObjectContent()))) {
                        boolean hasMore = true;
                        while (hasMore) {
                            String line = reader.readLine();
                            if (line != null) {
                                resultStr.append(line);
                            }
                            else {
                                hasMore = false;
                            }
                        }

                        KinesisStreamDescription table = streamDescriptionCodec.fromJson(resultStr.toString());

                        internalMap.put(objInfo.getKey(), table);
                        log.info("Put table description into the map from %s", objInfo.getKey());
                    }
                    catch (IOException iox) {
                        log.error("Problem reading input stream from object.", iox);
                    }
                }
                catch (AmazonServiceException ase) {
                    StringBuilder sb = new StringBuilder();
                    sb.append("Caught an AmazonServiceException, which means your request made it ");
                    sb.append("to Amazon S3, but was rejected with an error response for some reason.\n");
                    sb.append("Error Message:    " + ase.getMessage());
                    sb.append("HTTP Status Code: " + ase.getStatusCode());
                    sb.append("AWS Error Code:   " + ase.getErrorCode());
                    sb.append("Error Type:       " + ase.getErrorType());
                    sb.append("Request ID:       " + ase.getRequestId());
                    log.error(sb.toString(), ase);
                }
                catch (AmazonClientException ace) {
                    StringBuilder sb = new StringBuilder();
                    sb.append("Caught an AmazonClientException, " +
                            "which means the client encountered " +
                            "an internal error while trying to communicate" +
                            " with S3, " +
                            "such as not being able to access the network.");
                    sb.append("Error Message: " + ace.getMessage());
                    log.error(sb.toString(), ace);
                }
            }
        } // end loop through object descriptions

        log.info("Completed updating table definitions from S3.");
        this.lastCheck = now;

        return;
    }
}
