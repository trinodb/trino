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
package io.trino.tests.product.deltalake;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.Region;
import io.trino.testing.minio.MinioClient;

import static io.trino.testing.minio.MinioClient.DEFAULT_MINIO_ACCESS_KEY;
import static io.trino.testing.minio.MinioClient.DEFAULT_MINIO_SECRET_KEY;

final class S3ClientFactory
{
    public static final String AWS_S3_SERVER_TYPE = "aws";
    public static final String MINIO_S3_SERVER_TYPE = "minio";

    public AmazonS3 createS3Client(String serverType)
    {
        switch (serverType) {
            case AWS_S3_SERVER_TYPE:
                return createAwsS3Client();
            case MINIO_S3_SERVER_TYPE:
                return createMinioS3Client();
            default:
                throw new IllegalArgumentException("Invalid value '" + serverType + "' for the s3 server type");
        }
    }

    private AmazonS3 createAwsS3Client()
    {
        return AmazonS3Client.builder().withRegion(Regions.US_EAST_2.getName()).build();
    }

    private AmazonS3 createMinioS3Client()
    {
        AWSCredentials credentials = new BasicAWSCredentials(DEFAULT_MINIO_ACCESS_KEY, DEFAULT_MINIO_SECRET_KEY);
        AWSCredentialsProvider credentialsProvider = new AWSStaticCredentialsProvider(credentials);
        ClientConfiguration clientConfiguration = new ClientConfiguration()
                .withProtocol(Protocol.HTTP)
                .withSignerOverride("AWSS3V4SignerType");

        return AmazonS3Client.builder()
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(MinioClient.DEFAULT_MINIO_ENDPOINT, Region.US_East_2.name()))
                .withPathStyleAccessEnabled(true)
                .withClientConfiguration(clientConfiguration)
                .withCredentials(credentialsProvider)
                .build();
    }
}
