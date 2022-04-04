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
package io.trino.plugin.iceberg.catalog.dynamodb;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import io.trino.collect.cache.NonEvictableLoadingCache;
import io.trino.plugin.hive.HdfsEnvironment;
import io.trino.plugin.iceberg.FileIoProvider;
import io.trino.spi.connector.ConnectorSession;
import org.apache.iceberg.aws.AwsProperties;
import org.apache.iceberg.aws.dynamodb.IcebergDynamoDbCatalog;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClientBuilder;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;

import javax.inject.Inject;

import java.net.URI;
import java.util.Map;

import static io.trino.collect.cache.SafeCaches.buildNonEvictableCache;
import static java.util.Objects.requireNonNull;
import static org.apache.iceberg.aws.AwsProperties.DYNAMODB_TABLE_NAME;

public class DynamoDbClientFactory
{
    private final NonEvictableLoadingCache<ConnectorSession, DynamoDbIcebergClient> clientCache;
    private final FileIoProvider fileIoProvider;
    private final DynamoDbIcebergConfig config;

    @Inject
    public DynamoDbClientFactory(FileIoProvider fileIoProvider, DynamoDbIcebergConfig config)
    {
        this.fileIoProvider = requireNonNull(fileIoProvider, "fileIoProvider is null");
        this.config = requireNonNull(config, "config is null");
        this.clientCache = buildNonEvictableCache(CacheBuilder.newBuilder().maximumSize(1000), CacheLoader.from(this::createDynamoClient));
    }

    public DynamoDbIcebergClient create(ConnectorSession session)
    {
        return clientCache.getUnchecked(session);
    }

    private DynamoDbIcebergClient createDynamoClient(ConnectorSession session)
    {
        IcebergDynamoDbCatalog dynamoCatalog = new IcebergDynamoDbCatalog();
        dynamoCatalog.initialize(
                config.getCatalogName(),
                config.getDefaultWarehouseDir(),
                new AwsProperties(Map.of(DYNAMODB_TABLE_NAME, config.getTableName())),
                getDynamoDbClient(config),
                fileIoProvider.createFileIo(new HdfsEnvironment.HdfsContext(session), session.getQueryId()));

        return new DynamoDbIcebergClient(dynamoCatalog);
    }

    private static DynamoDbClient getDynamoDbClient(DynamoDbIcebergConfig config)
    {
        DynamoDbClientBuilder builder = DynamoDbClient.builder();
        config.getConnectionUrl().ifPresent(url -> builder.endpointOverride(URI.create(url)));
        builder.credentialsProvider(getAwsCredentialsProvider(config));
        return builder.build();
    }

    private static AwsCredentialsProvider getAwsCredentialsProvider(DynamoDbIcebergConfig config)
    {
        AwsCredentialsProvider credentialsProvider = DefaultCredentialsProvider.create();

        if (config.getAccessKey().isPresent() && config.getSecretKey().isPresent()) {
            credentialsProvider = StaticCredentialsProvider.create(AwsBasicCredentials.create(
                    config.getAccessKey().get(),
                    config.getSecretKey().get()));
        }

        if (config.getIamRole().isPresent()) {
            AssumeRoleRequest.Builder assumeRoleRequest = AssumeRoleRequest.builder()
                    .roleArn(config.getIamRole().get())
                    .roleSessionName("trino-session");
            config.getExternalId().ifPresent(assumeRoleRequest::externalId);

            StsAssumeRoleCredentialsProvider.Builder credentialsProviderBuilder = StsAssumeRoleCredentialsProvider.builder()
                    .stsClient(StsClient.builder()
                            .region(Region.of(config.getRegion()))
                            .credentialsProvider(credentialsProvider)
                            .build())
                    .refreshRequest(assumeRoleRequest.build());
            credentialsProvider = credentialsProviderBuilder.build();
        }

        return credentialsProvider;
    }
}
