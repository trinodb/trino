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
package io.trino.plugin.iceberg;

import com.google.common.collect.ImmutableMap;
import io.trino.plugin.base.util.AutoCloseableCloser;
import io.trino.testing.containers.Floci;
import io.trino.testing.containers.IcebergS3RestCatalogBackendContainer;
import org.testcontainers.containers.Network;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;
import software.amazon.awssdk.services.sts.model.AssumeRoleResponse;

import java.util.Optional;

import static io.trino.testing.containers.Floci.FLOCI_PORT;
import static io.trino.testing.containers.Floci.FLOCI_REGION;
import static io.trino.testing.containers.TestContainers.getPathFromClassPathResource;

public final class SparkIcebergRestFlociDataLake
        implements AutoCloseable
{
    private final AutoCloseableCloser closer = AutoCloseableCloser.create();
    private final Floci floci;
    private final IcebergS3RestCatalogBackendContainer restCatalogBackendContainer;
    private final SparkIceberg spark;
    private final String accessKey;
    private final String secretKey;

    public SparkIcebergRestFlociDataLake(String bucketName)
    {
        Network network = closer.register(Network.newNetwork());
        floci = closer.register(new Floci().withNetwork(network).withNetworkAliases("floci"));
        floci.start();
        floci.createBucket(bucketName);

        String warehouseLocation = "s3://%s/default/".formatted(bucketName);

        AssumeRoleResponse assumeRoleResponse;
        try (StsClient stsClient = StsClient.builder().applyMutation(floci::updateClient).build()) {
            assumeRoleResponse = stsClient.assumeRole(AssumeRoleRequest.builder()
                    .roleArn("arn:aws:iam::000000000000:role/iceberg")
                    .roleSessionName("iceberg-rest-spark")
                    .build());
        }

        accessKey = assumeRoleResponse.credentials().accessKeyId();
        secretKey = assumeRoleResponse.credentials().secretAccessKey();

        restCatalogBackendContainer = closer.register(new IcebergS3RestCatalogBackendContainer(
                Optional.of(network),
                warehouseLocation,
                assumeRoleResponse.credentials().accessKeyId(),
                assumeRoleResponse.credentials().secretAccessKey(),
                assumeRoleResponse.credentials().sessionToken(),
                "http://floci:" + FLOCI_PORT,
                FLOCI_REGION));
        restCatalogBackendContainer.start();

        SparkIceberg.Builder sparkIcebergBuilder = SparkIceberg.builder()
                .withNetwork(network)
                .withFilesToMount(ImmutableMap.of(
                        "/spark/conf/spark-defaults.conf", getPathFromClassPathResource("spark/rest/spark-defaults.conf"),
                        "/spark/conf/log4j2.properties", getPathFromClassPathResource("spark/log4j2.properties")))
                .withEnvVars(ImmutableMap.of(
                        "AWS_ACCESS_KEY_ID", assumeRoleResponse.credentials().accessKeyId(),
                        "AWS_SECRET_ACCESS_KEY", assumeRoleResponse.credentials().secretAccessKey(),
                        "AWS_SESSION_TOKEN", assumeRoleResponse.credentials().sessionToken(),
                        "AWS_REGION", FLOCI_REGION));
        spark = closer.register(sparkIcebergBuilder.build());
        spark.start();
    }

    public Floci floci()
    {
        return floci;
    }

    public IcebergS3RestCatalogBackendContainer restCatalogBackendContainer()
    {
        return restCatalogBackendContainer;
    }

    public String accessKey()
    {
        return accessKey;
    }

    public String secretKey()
    {
        return secretKey;
    }

    @Override
    public void close()
            throws Exception
    {
        closer.close();
    }
}
