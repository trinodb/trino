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
package io.trino.plugin.iceberg.catalog.rest;

import com.google.inject.Binder;
import com.google.inject.Scopes;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.filesystem.manager.FileSystemConfig;
import io.trino.plugin.iceberg.IcebergConfig;
import io.trino.plugin.iceberg.IcebergFileSystemFactory;
import io.trino.plugin.iceberg.catalog.TrinoCatalogFactory;
import io.trino.spi.TrinoException;

import java.net.URI;
import java.util.Optional;

import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.trino.spi.StandardErrorCode.CONFIGURATION_INVALID;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;

public class IcebergRestCatalogModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        configBinder(binder).bindConfig(IcebergRestCatalogConfig.class);
        IcebergRestCatalogConfig icebergRestCatalogConfig = buildConfigObject(IcebergRestCatalogConfig.class);
        install(switch (icebergRestCatalogConfig.getSecurity()) {
            case OAUTH2 -> new OAuth2SecurityModule();
            case SIGV4 -> new SigV4SecurityModule();
            case GOOGLE -> new GoogleSecurityModule();
            case NONE -> new NoneSecurityModule();
        });

        binder.bind(IcebergRestCatalogPropertiesProvider.class).in(Scopes.SINGLETON);
        binder.bind(TrinoCatalogFactory.class).to(TrinoIcebergRestCatalogFactory.class).in(Scopes.SINGLETON);
        newOptionalBinder(binder, IcebergFileSystemFactory.class).setBinding().to(IcebergRestCatalogFileSystemFactory.class).in(Scopes.SINGLETON);

        validateConfiguration(icebergRestCatalogConfig);
    }

    private void validateConfiguration(IcebergRestCatalogConfig restCatalogConfig)
    {
        if (restCatalogConfig.isVendedCredentialsEnabled()) {
            IcebergConfig icebergConfig = buildConfigObject(IcebergConfig.class);
            if (icebergConfig.isRegisterTableProcedureEnabled()) {
                throw new TrinoException(NOT_SUPPORTED, "Using the `register_table` procedure with vended credentials is currently not supported");
            }
            FileSystemConfig fileSystemConfig = buildConfigObject(FileSystemConfig.class);
            Optional<String> warehouseScheme = restCatalogConfig.getWarehouse().flatMap(warehouse -> {
                try {
                    return Optional.ofNullable(URI.create(warehouse).getScheme());
                }
                catch (IllegalArgumentException _) {
                    return Optional.empty();
                }
            });
            if (warehouseScheme.isPresent()) {
                validateFileSystemEnabledForScheme(warehouseScheme.orElseThrow(), fileSystemConfig);
                return;
            }
            if (!fileSystemConfig.isS3Enabled() && !fileSystemConfig.isGcsEnabled() && !fileSystemConfig.isAzureEnabled()) {
                throw new TrinoException(
                        CONFIGURATION_INVALID,
                        "Vended credentials require a native cloud filesystem to be enabled (set fs.s3.enabled, fs.gcs.enabled, or fs.azure.enabled to true, or disable iceberg.rest-catalog.vended-credentials-enabled)");
            }
        }
    }

    private static void validateFileSystemEnabledForScheme(String scheme, FileSystemConfig fileSystemConfig)
    {
        switch (scheme) {
            case "s3", "s3a", "s3n" -> {
                if (!fileSystemConfig.isS3Enabled()) {
                    throw new TrinoException(
                            CONFIGURATION_INVALID,
                            "Vended credentials require fs.s3.enabled=true when warehouse location uses the s3 scheme (or disable iceberg.rest-catalog.vended-credentials-enabled)");
                }
            }
            case "gs" -> {
                if (!fileSystemConfig.isGcsEnabled()) {
                    throw new TrinoException(
                            CONFIGURATION_INVALID,
                            "Vended credentials require fs.gcs.enabled=true when warehouse location uses the gs scheme (or disable iceberg.rest-catalog.vended-credentials-enabled)");
                }
            }
            case "abfs", "abfss", "wasb", "wasbs" -> {
                if (!fileSystemConfig.isAzureEnabled()) {
                    throw new TrinoException(
                            CONFIGURATION_INVALID,
                            "Vended credentials require fs.azure.enabled=true when warehouse location uses the %s scheme (or disable iceberg.rest-catalog.vended-credentials-enabled)".formatted(scheme));
                }
            }
            default -> throw new TrinoException(CONFIGURATION_INVALID, "Vended credentials do not support warehouse locations with the %s scheme".formatted(scheme));
        }
    }
}
