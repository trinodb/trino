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
package io.trino.plugin.iceberg.catalog.glue;

import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.s3.LakeFormationCredentialProvider;
import io.trino.plugin.hive.metastore.glue.GlueHiveMetastoreConfig;
import io.trino.plugin.hive.metastore.glue.GlueMetastoreStats;
import io.trino.plugin.iceberg.catalog.IcebergTableOperations;
import io.trino.plugin.iceberg.catalog.IcebergTableOperationsProvider;
import io.trino.plugin.iceberg.catalog.TrinoCatalog;
import io.trino.plugin.iceberg.fileio.ForwardingFileIoFactory;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.spi.type.TypeManager;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.services.glue.GlueClient;

import java.util.Optional;

import static io.trino.filesystem.s3.S3FileSystemConstants.EXTRA_CREDENTIALS_ACCESS_KEY_PROPERTY;
import static io.trino.filesystem.s3.S3FileSystemConstants.EXTRA_CREDENTIALS_SECRET_KEY_PROPERTY;
import static io.trino.filesystem.s3.S3FileSystemConstants.EXTRA_CREDENTIALS_SESSION_TOKEN_PROPERTY;
import static io.trino.plugin.iceberg.IcebergSessionProperties.isUseFileSizeFromMetadata;
import static java.util.Objects.requireNonNull;

public class GlueIcebergTableOperationsProvider
        implements IcebergTableOperationsProvider
{
    private final TrinoFileSystemFactory fileSystemFactory;
    private final ForwardingFileIoFactory fileIoFactory;
    private final TypeManager typeManager;
    private final boolean cacheTableMetadata;
    private final GlueClient glueClient;
    private final GlueMetastoreStats stats;
    private final Optional<LakeFormationCredentialProvider> lakeFormationCredentialProvider;
    private final Optional<String> glueCatalogId;
    private final Optional<String> glueRegion;

    @Inject
    public GlueIcebergTableOperationsProvider(
            TrinoFileSystemFactory fileSystemFactory,
            ForwardingFileIoFactory fileIoFactory,
            TypeManager typeManager,
            IcebergGlueCatalogConfig catalogConfig,
            GlueHiveMetastoreConfig glueConfig,
            GlueMetastoreStats stats,
            GlueClient glueClient,
            Optional<LakeFormationCredentialProvider> lakeFormationCredentialProvider)
    {
        this.fileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
        this.fileIoFactory = requireNonNull(fileIoFactory, "fileIoFactory is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.cacheTableMetadata = catalogConfig.isCacheTableMetadata();
        this.stats = requireNonNull(stats, "stats is null");
        this.glueClient = requireNonNull(glueClient, "glueClient is null");
        this.lakeFormationCredentialProvider = requireNonNull(lakeFormationCredentialProvider, "lakeFormationCredentialProvider is null");
        this.glueCatalogId = glueConfig.getCatalogId();
        this.glueRegion = glueConfig.getGlueRegion();
    }

    @Override
    public IcebergTableOperations createTableOperations(
            TrinoCatalog catalog,
            ConnectorSession session,
            String database,
            String table,
            Optional<String> owner,
            Optional<String> location)
    {
        ConnectorIdentity identity = enrichIdentityWithLakeFormationCredentials(session.getIdentity(), database, table);

        return new GlueIcebergTableOperations(
                typeManager,
                cacheTableMetadata,
                glueClient,
                stats,
                // Share Glue Table cache between Catalog and TableOperations so that, when doing metadata queries (e.g. information_schema.columns)
                // the GetTableRequest is issued once per table.
                ((TrinoGlueCatalog) catalog)::getTable,
                fileIoFactory.create(fileSystemFactory.create(identity), isUseFileSizeFromMetadata(session)),
                session,
                database,
                table,
                owner,
                location);
    }

    private ConnectorIdentity enrichIdentityWithLakeFormationCredentials(ConnectorIdentity identity, String database, String table)
    {
        if (lakeFormationCredentialProvider.isEmpty() || glueCatalogId.isEmpty()) {
            return identity;
        }

        String region = glueRegion.orElse("us-east-1");
        String tableArn = "arn:aws:glue:" + region + ":" + glueCatalogId.get() + ":table/" + database + "/" + table;

        AwsCredentials credentials = lakeFormationCredentialProvider.get()
                .getCredentialsProvider(tableArn)
                .resolveCredentials();

        if (!(credentials instanceof AwsSessionCredentials sessionCredentials)) {
            return identity;
        }

        return ConnectorIdentity.forUser(identity.getUser())
                .withGroups(identity.getGroups())
                .withPrincipal(identity.getPrincipal())
                .withEnabledSystemRoles(identity.getEnabledSystemRoles())
                .withConnectorRole(identity.getConnectorRole())
                .withExtraCredentials(ImmutableMap.<String, String>builder()
                        .putAll(identity.getExtraCredentials())
                        .put(EXTRA_CREDENTIALS_ACCESS_KEY_PROPERTY, sessionCredentials.accessKeyId())
                        .put(EXTRA_CREDENTIALS_SECRET_KEY_PROPERTY, sessionCredentials.secretAccessKey())
                        .put(EXTRA_CREDENTIALS_SESSION_TOKEN_PROPERTY, sessionCredentials.sessionToken())
                        .buildOrThrow())
                .build();
    }
}
