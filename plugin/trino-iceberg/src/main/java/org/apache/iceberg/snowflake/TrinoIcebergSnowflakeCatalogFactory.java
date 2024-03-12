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
package org.apache.iceberg.snowflake;

import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.plugin.iceberg.catalog.IcebergTableOperationsProvider;
import io.trino.plugin.iceberg.catalog.TrinoCatalog;
import io.trino.plugin.iceberg.catalog.TrinoCatalogFactory;
import io.trino.plugin.iceberg.catalog.snowflake.IcebergSnowflakeCatalogConfig;
import io.trino.plugin.iceberg.catalog.snowflake.TrinoSnowflakeCatalog;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.spi.type.TypeManager;
import jakarta.annotation.PreDestroy;
import org.apache.iceberg.IcebergBuild;
import org.apache.iceberg.jdbc.JdbcClientPool;

import java.net.URI;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static java.util.Objects.requireNonNull;
import static org.apache.iceberg.jdbc.JdbcCatalog.PROPERTY_PREFIX;

public class TrinoIcebergSnowflakeCatalogFactory
        implements TrinoCatalogFactory
{
    // Below property values are copied from https://github.com/apache/iceberg/blob/apache-iceberg-1.5.0/snowflake/src/main/java/org/apache/iceberg/snowflake/SnowflakeCatalog.java#L122-L129
    private static final String JDBC_APPLICATION_PROPERTY = "application";
    private static final String APP_IDENTIFIER = "iceberg-snowflake-catalog";
    private static final String JDBC_USER_AGENT_SUFFIX_PROPERTY = "user_agent_suffix";
    private static final int UNIQUE_ID_LENGTH = 20;

    private final TrinoFileSystemFactory fileSystemFactory;
    private final CatalogName catalogName;
    private final TypeManager typeManager;
    private final IcebergTableOperationsProvider tableOperationsProvider;

    private final String snowflakeDatabase;
    private final Map<String, String> snowflakeDriverProperties;
    private final JdbcClientPool snowflakeConnectionPool;

    @Inject
    public TrinoIcebergSnowflakeCatalogFactory(
            TrinoFileSystemFactory fileSystemFactory,
            CatalogName catalogName,
            IcebergSnowflakeCatalogConfig snowflakeCatalogConfig,
            TypeManager typeManager,
            IcebergTableOperationsProvider tableOperationsProvider)
    {
        this.fileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
        this.catalogName = requireNonNull(catalogName, "catalogName is null");

        this.snowflakeDriverProperties = getSnowflakeDriverProperties(
                snowflakeCatalogConfig.getUri(),
                snowflakeCatalogConfig.getUser(),
                snowflakeCatalogConfig.getPassword(),
                snowflakeCatalogConfig.getRole());
        this.snowflakeDatabase = snowflakeCatalogConfig.getDatabase();
        this.snowflakeConnectionPool = new JdbcClientPool(snowflakeCatalogConfig.getUri().toString(), snowflakeDriverProperties);

        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.tableOperationsProvider = requireNonNull(tableOperationsProvider, "tableOperationsProvider is null");
    }

    @PreDestroy
    public void shutdown()
    {
        snowflakeConnectionPool.close();
    }

    @Override
    public synchronized TrinoCatalog create(ConnectorIdentity identity)
    {
        // Mimics the `SnowflakeCatalog` initialization https://github.com/apache/iceberg/blob/apache-iceberg-1.5.0/snowflake/src/main/java/org/apache/iceberg/snowflake/SnowflakeCatalog.java#L105-L134
        SnowflakeClient snowflakeClient = new JdbcSnowflakeClient(snowflakeConnectionPool);
        SnowflakeCatalog icebergSnowflakeCatalog = new SnowflakeCatalog();
        icebergSnowflakeCatalog.initialize(catalogName.toString(), snowflakeClient, new TrinoIcebergSnowflakeCatalogFileIOFactory(fileSystemFactory, identity), snowflakeDriverProperties);

        return new TrinoSnowflakeCatalog(icebergSnowflakeCatalog, catalogName, typeManager, fileSystemFactory, tableOperationsProvider, snowflakeDatabase);
    }

    public static Map<String, String> getSnowflakeDriverProperties(URI snowflakeUri, String snowflakeUser, String snowflakePassword, Optional<String> snowflakeRole)
    {
        // Below property values are copied from https://github.com/apache/iceberg/blob/apache-iceberg-1.5.0/snowflake/src/main/java/org/apache/iceberg/snowflake/SnowflakeCatalog.java#L122-L129

        // The uniqueAppIdentifier should be less than 50 characters.
        String uniqueId = UUID.randomUUID().toString().replace("-", "").substring(0, UNIQUE_ID_LENGTH);
        String uniqueAppIdentifier = APP_IDENTIFIER + "_" + uniqueId;
        String userAgentSuffix = IcebergBuild.fullVersion() + " " + uniqueAppIdentifier;

        ImmutableMap.Builder<String, String> properties = ImmutableMap.builder();
        properties
                .put(PROPERTY_PREFIX + "user", snowflakeUser)
                .put(PROPERTY_PREFIX + "password", snowflakePassword)
                .put("uri", snowflakeUri.toString())
                // Populate application identifier in jdbc client
                .put(PROPERTY_PREFIX + JDBC_APPLICATION_PROPERTY, uniqueAppIdentifier)
                // Adds application identifier to the user agent header of the JDBC requests.
                .put(PROPERTY_PREFIX + JDBC_USER_AGENT_SUFFIX_PROPERTY, userAgentSuffix);
        snowflakeRole.ifPresent(role -> properties.put(PROPERTY_PREFIX + "role", role));

        return properties.buildOrThrow();
    }
}
