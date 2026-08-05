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
package io.trino.testing.containers;

import com.google.common.reflect.ClassPath;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.awscore.client.builder.AwsClientBuilder;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.util.List;
import java.util.regex.Pattern;

import static java.util.regex.Matcher.quoteReplacement;

public final class Floci
        extends GenericContainer<Floci>
{
    public static final String FLOCI_IMAGE = "floci/floci:1.5.26";
    public static final String FLOCI_ACCESS_KEY = "floci-access-key";
    public static final String FLOCI_SECRET_KEY = "floci-secret-key";
    public static final String FLOCI_REGION = "us-east-1";
    public static final int FLOCI_PORT = 4566;

    public Floci()
    {
        super(DockerImageName.parse(FLOCI_IMAGE));
        addExposedPort(FLOCI_PORT);
        waitingFor(Wait.forHttp("/_floci/init").forPort(FLOCI_PORT));
    }

    public URI endpoint()
    {
        return URI.create("http://%s:%s".formatted(getHost(), getMappedPort(FLOCI_PORT)));
    }

    public void updateClient(AwsClientBuilder<?, ?> client)
    {
        client.endpointOverride(endpoint());
        client.region(Region.of(FLOCI_REGION));
        client.credentialsProvider(StaticCredentialsProvider.create(
                AwsBasicCredentials.create(FLOCI_ACCESS_KEY, FLOCI_SECRET_KEY)));
        if (client instanceof S3ClientBuilder s3) {
            s3.forcePathStyle(true);
        }
    }

    public S3Client createS3Client()
    {
        return S3Client.builder().applyMutation(this::updateClient).build();
    }

    public void createBucket(String bucketName)
    {
        try (S3Client s3 = createS3Client()) {
            s3.createBucket(builder -> builder.bucket(bucketName));
        }
    }

    public void putObject(String bucketName, byte[] contents, String key)
    {
        try (S3Client s3 = createS3Client()) {
            s3.putObject(
                    builder -> builder.bucket(bucketName).key(key),
                    RequestBody.fromBytes(contents));
        }
    }

    public byte[] getObjectContents(String bucketName, String key)
    {
        try (S3Client s3 = createS3Client()) {
            return s3.getObjectAsBytes(builder -> builder.bucket(bucketName).key(key)).asByteArray();
        }
    }

    public void copyResources(String resourcePath, String bucketName, String target)
    {
        try (S3Client s3 = createS3Client()) {
            for (ClassPath.ResourceInfo resourceInfo : ClassPath.from(getClass().getClassLoader()).getResources()) {
                if (resourceInfo.getResourceName().startsWith(resourcePath)) {
                    String fileName = resourceInfo.getResourceName().replaceFirst("^" + Pattern.quote(resourcePath), quoteReplacement(target));
                    s3.putObject(
                            builder -> builder.bucket(bucketName).key(fileName),
                            RequestBody.fromBytes(resourceInfo.asByteSource().read()));
                }
            }
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public List<String> listObjects(String bucketName, String prefix)
    {
        try (S3Client s3 = createS3Client()) {
            return s3.listObjectsV2Paginator(builder -> builder.bucket(bucketName).prefix(prefix))
                    .contents()
                    .stream()
                    .map(S3Object::key)
                    .toList();
        }
    }

    public void copyObject(String sourceBucketName, String sourceKey, String destinationBucketName, String destinationKey)
    {
        try (S3Client s3 = createS3Client()) {
            s3.copyObject(builder -> builder
                    .sourceBucket(sourceBucketName)
                    .sourceKey(sourceKey)
                    .destinationBucket(destinationBucketName)
                    .destinationKey(destinationKey));
        }
    }

    public void deleteObject(String bucketName, String key)
    {
        try (S3Client s3 = createS3Client()) {
            s3.deleteObject(builder -> builder.bucket(bucketName).key(key));
        }
    }

    public void deleteObjects(String bucketName, String prefix)
    {
        try (S3Client s3 = createS3Client()) {
            s3.listObjectsV2Paginator(builder -> builder.bucket(bucketName).prefix(prefix))
                    .contents()
                    .stream()
                    .map(S3Object::key)
                    .forEach(key -> s3.deleteObject(builder -> builder.bucket(bucketName).key(key)));
        }
    }
}
