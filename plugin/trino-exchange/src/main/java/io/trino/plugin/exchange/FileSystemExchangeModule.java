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
package io.trino.plugin.exchange;

import com.google.common.collect.ImmutableSet;
import com.google.inject.Binder;
import com.google.inject.Scopes;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.plugin.exchange.local.LocalFileSystemExchangeStorage;
import io.trino.plugin.exchange.s3.ExchangeS3Config;
import io.trino.plugin.exchange.s3.S3FileSystemExchangeStorage;
import io.trino.spi.TrinoException;

import java.net.URI;

import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class FileSystemExchangeModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        binder.bind(FileSystemExchangeManager.class).in(Scopes.SINGLETON);

        FileSystemExchangeConfig fileSystemExchangeConfig = buildConfigObject(FileSystemExchangeConfig.class);
        String scheme = URI.create(requireNonNull(fileSystemExchangeConfig.getBaseDirectory(), "baseDirectory is null")).getScheme();
        if (scheme == null || scheme.equals("file")) {
            binder.bind(FileSystemExchangeStorage.class).to(LocalFileSystemExchangeStorage.class).in(Scopes.SINGLETON);
        }
        else if (ImmutableSet.of("s3", "s3a", "s3n").contains(scheme)) {
            binder.bind(FileSystemExchangeStorage.class).to(S3FileSystemExchangeStorage.class).in(Scopes.SINGLETON);
            configBinder(binder).bindConfig(ExchangeS3Config.class);
        }
        else {
            throw new TrinoException(NOT_SUPPORTED, format("Scheme %s is not supported as exchange spooling storage", scheme));
        }
    }
}
