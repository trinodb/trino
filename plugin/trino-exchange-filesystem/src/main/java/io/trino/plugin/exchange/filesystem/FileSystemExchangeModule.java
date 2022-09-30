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
package io.trino.plugin.exchange.filesystem;

import com.google.common.collect.ImmutableSet;
import com.google.inject.Binder;
import com.google.inject.Scopes;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.plugin.exchange.filesystem.azure.AzureBlobFileSystemExchangeStorage;
import io.trino.plugin.exchange.filesystem.azure.ExchangeAzureConfig;
import io.trino.plugin.exchange.filesystem.local.LocalFileSystemExchangeStorage;
import io.trino.plugin.exchange.filesystem.s3.ExchangeS3Config;
import io.trino.plugin.exchange.filesystem.s3.S3FileSystemExchangeStorage;
import io.trino.plugin.exchange.filesystem.s3.S3FileSystemExchangeStorageStats;
import io.trino.spi.TrinoException;

import java.net.URI;
import java.util.List;

import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.trino.plugin.exchange.filesystem.s3.S3FileSystemExchangeStorage.CompatibilityMode.AWS;
import static io.trino.plugin.exchange.filesystem.s3.S3FileSystemExchangeStorage.CompatibilityMode.GCP;
import static io.trino.spi.StandardErrorCode.CONFIGURATION_INVALID;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.lang.String.format;
import static org.weakref.jmx.guice.ExportBinder.newExporter;

public class FileSystemExchangeModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        binder.bind(FileSystemExchangeStats.class).in(Scopes.SINGLETON);
        newExporter(binder).export(FileSystemExchangeStats.class).withGeneratedName();

        binder.bind(FileSystemExchangeManager.class).in(Scopes.SINGLETON);

        List<URI> baseDirectories = buildConfigObject(FileSystemExchangeConfig.class).getBaseDirectories();
        if (baseDirectories.stream().map(URI::getScheme).distinct().count() != 1) {
            binder.addError(new TrinoException(CONFIGURATION_INVALID, "Multiple schemes in exchange base directories"));
            return;
        }
        String scheme = baseDirectories.get(0).getScheme();
        if (scheme == null || scheme.equals("file")) {
            binder.bind(FileSystemExchangeStorage.class).to(LocalFileSystemExchangeStorage.class).in(Scopes.SINGLETON);
        }
        else if (ImmutableSet.of("s3", "gs").contains(scheme)) {
            binder.bind(S3FileSystemExchangeStorageStats.class).in(Scopes.SINGLETON);
            newExporter(binder).export(S3FileSystemExchangeStorageStats.class).withGeneratedName();
            binder.bind(FileSystemExchangeStorage.class).to(S3FileSystemExchangeStorage.class).in(Scopes.SINGLETON);
            configBinder(binder).bindConfig(ExchangeS3Config.class);
            S3FileSystemExchangeStorage.CompatibilityMode compatibilityMode = scheme.equals("gs") ? GCP : AWS;
            binder.bind(S3FileSystemExchangeStorage.CompatibilityMode.class).toInstance(compatibilityMode);
        }
        else if (ImmutableSet.of("abfs", "abfss").contains(scheme)) {
            binder.bind(FileSystemExchangeStorage.class).to(AzureBlobFileSystemExchangeStorage.class).in(Scopes.SINGLETON);
            configBinder(binder).bindConfig(ExchangeAzureConfig.class);
        }
        else {
            binder.addError(new TrinoException(NOT_SUPPORTED,
                    format("Scheme %s is not supported as exchange spooling storage in exchange manager type %s", scheme, FileSystemExchangeManagerFactory.FILESYSTEM)));
        }
    }
}
