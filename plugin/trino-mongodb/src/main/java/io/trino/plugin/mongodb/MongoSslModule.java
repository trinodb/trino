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
package io.trino.plugin.mongodb;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Singleton;
import com.google.inject.multibindings.ProvidesIntoSet;
import io.trino.spi.TrinoException;

import javax.net.ssl.SSLContext;

import java.io.File;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.Optional;

import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.trino.plugin.base.ssl.SslUtils.createSSLContext;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;

public class MongoSslModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        configBinder(binder).bindConfig(MongoSslConfig.class);
    }

    @ProvidesIntoSet
    @Singleton
    public MongoClientSettingConfigurator sslSpecificConfigurator(MongoSslConfig config)
    {
        return options -> options.applyToSslSettings(
                builder -> {
                    builder.enabled(true);
                    buildSslContext(config.getKeystorePath(), config.getKeystorePassword(), config.getTruststorePath(), config.getTruststorePassword())
                            .ifPresent(builder::context);
                });
    }

    // TODO https://github.com/trinodb/trino/issues/15247 Add test for x.509 certificates
    private static Optional<SSLContext> buildSslContext(
            Optional<File> keystorePath,
            Optional<String> keystorePassword,
            Optional<File> truststorePath,
            Optional<String> truststorePassword)
    {
        if (keystorePath.isEmpty() && truststorePath.isEmpty()) {
            return Optional.empty();
        }

        try {
            return Optional.of(createSSLContext(keystorePath, keystorePassword, truststorePath, truststorePassword));
        }
        catch (GeneralSecurityException | IOException e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, e);
        }
    }
}
