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
package io.trino.testing.ceph;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.iterable.S3Objects;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.google.common.collect.Sets;
import com.google.common.collect.Streams;
import com.google.common.io.ByteSource;
import com.google.common.reflect.ClassPath;
import io.airlift.log.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static java.util.regex.Matcher.quoteReplacement;

public class CephClient
{
    private static final Logger log = Logger.get(CephClient.class);

    private static final Set<String> createdBuckets = Sets.newConcurrentHashSet();

    private final AmazonS3 client;

    public CephClient(String endpoint, String accessKey, String secretKey)
    {
        AWSCredentials credentials = new BasicAWSCredentials(accessKey, secretKey);
        AWSCredentialsProvider credentialsProvider = new AWSStaticCredentialsProvider(credentials);
        ClientConfiguration clientConfiguration = new ClientConfiguration()
                .withProtocol(Protocol.HTTP)
                .withSignerOverride("AWSS3V4SignerType");

        client = AmazonS3ClientBuilder
                .standard()
                .withCredentials(credentialsProvider)
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(endpoint, Regions.US_EAST_1.name()))
                .withPathStyleAccessEnabled(true)
                .withClientConfiguration(clientConfiguration)
                .build();
    }

    public void copyResourcePath(String bucket, String resourcePath, String target)
    {
        ensureBucketExists(bucket);

        try {
            ClassPath.from(CephClient.class.getClassLoader())
                    .getResources().stream()
                    .filter(resourceInfo -> resourceInfo.getResourceName().startsWith(resourcePath))
                    .forEach(resourceInfo -> {
                        String fileName = resourceInfo.getResourceName().replaceFirst("^" + Pattern.quote(resourcePath), quoteReplacement(target));
                        try {
                            putObject(bucket, fileName, resourceInfo.asByteSource());
                        }
                        catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    });
        }
        catch (IOException e) {
            log.warn(e, "Could not copy resources from classpath");
            throw new UncheckedIOException(e);
        }
    }

    private void putObject(String bucket, String fileName, ByteSource byteSource)
            throws IOException
    {
        ensureBucketExists(bucket);

        try (InputStream inputStream = byteSource.openStream()) {
            client.putObject(new PutObjectRequest(bucket, fileName, inputStream, new ObjectMetadata()));
        }
    }

    public List<String> listObjects(String bucket, String path)
    {
        return Streams
                .stream(S3Objects.withPrefix(client, bucket, path))
                .map(S3ObjectSummary::getKey).collect(Collectors.toList());
    }

    public void makeBucket(String bucket)
    {
        if (!createdBuckets.add(bucket)) {
            // Forbid to create a bucket with given name more than once per class loader.
            // The reason for that is that bucket name is used as a key in TrinoFileSystemCache which is
            // managed in static manner. Allowing creating same bucket twice (even for two different ceph environments)
            // leads to hard to understand problems when one test influences the other via unexpected cache lookups.
            throw new IllegalArgumentException("Bucket " + bucket + " already created in this classloader");
        }
        try {
            client.createBucket(bucket);
        }
        catch (Exception e) {
            // Revert bucket registration, so we can retry the call on transient errors.
            createdBuckets.remove(bucket);
            throw new RuntimeException(e);
        }
    }

    public void ensureBucketExists(String bucket)
    {
        try {
            if (!client.doesBucketExistV2(bucket)) {
                makeBucket(bucket);
            }
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
