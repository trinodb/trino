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

import org.testcontainers.containers.GenericContainer;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.awscore.client.builder.AwsClientBuilder;
import software.amazon.awssdk.core.checksums.RequestChecksumCalculation;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.LegacyMd5Plugin;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;

import java.net.URI;

import static software.amazon.awssdk.core.checksums.ResponseChecksumValidation.WHEN_REQUIRED;

public final class MotoContainer
        extends GenericContainer<MotoContainer>
{
    public static final String MOTO_ACCESS_KEY = "accesskey";
    public static final String MOTO_SECRET_KEY = "secretkey";
    public static final String MOTO_REGION = "us-east-1";

    private static final int MOTO_PORT = 5000;

    public MotoContainer()
    {
        super("motoserver/moto:latest");
        addExposedPort(MOTO_PORT);
    }

    public URI getEndpoint()
    {
        return URI.create("http://" + getHost() + ":" + getMappedPort(MOTO_PORT));
    }

    public void updateClient(AwsClientBuilder<?, ?> client)
    {
        client.endpointOverride(getEndpoint());
        client.region(Region.of(MOTO_REGION));
        client.credentialsProvider(StaticCredentialsProvider.create(
                AwsBasicCredentials.create(MOTO_ACCESS_KEY, MOTO_SECRET_KEY)));
        if (client instanceof S3ClientBuilder s3) {
            s3.forcePathStyle(true);
            s3.responseChecksumValidation(WHEN_REQUIRED);
            s3.requestChecksumCalculation(RequestChecksumCalculation.WHEN_REQUIRED);
        }
        client.addPlugin(LegacyMd5Plugin.create());
    }

    public void createBucket(String bucketName)
    {
        try (S3Client s3 = S3Client.builder().applyMutation(this::updateClient).build()) {
            s3.createBucket(builder -> builder.bucket(bucketName));
        }
    }
}
