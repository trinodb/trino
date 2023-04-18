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
package io.trino.plugin.pinot.deepstore.s3;

import com.google.common.collect.ImmutableMap;
import com.google.inject.Binder;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.plugin.pinot.deepstore.DeepStore;
import io.trino.plugin.pinot.deepstore.PinotDeepStore;
import io.trino.plugin.pinot.deepstore.PinotDeepStoreConfig;
import org.apache.pinot.spi.env.PinotConfiguration;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static java.nio.charset.StandardCharsets.UTF_8;

public class PinotS3Module
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        configBinder(binder).bindConfig(PinotS3Config.class);
        configBinder(binder).bindConfig(PinotDeepStoreConfig.class);
        newOptionalBinder(binder, DeepStore.class).setBinding().to(PinotDeepStore.class).in(Scopes.SINGLETON);
    }

    @Provides
    @Singleton
    public static PinotConfiguration getPinotFsConfiguration(PinotS3Config config)
    {
        try {
            ImmutableMap.Builder<String, Object> pinotConfigurationBuilder = ImmutableMap.builder();
            if (config.getS3AccessKeyFile().isPresent() && config.getS3SecretKeyFile().isPresent()) {
                pinotConfigurationBuilder
                        .put("storage.factory.s3.accesskey", Files.readString(Paths.get(config.getS3AccessKeyFile().get()), UTF_8))
                        .put("storage.factory.s3.secretkey", Files.readString(Paths.get(config.getS3SecretKeyFile().get()), UTF_8));
            }
            return new PinotConfiguration(pinotConfigurationBuilder
                    .put("storage.factory.class.s3", "org.apache.pinot.plugin.filesystem.S3PinotFS")
                    .put("storage.factory.s3.region", config.getS3Region().orElseThrow(() -> new IllegalStateException("s3 region is not present")))
                    .put("storage.factory.s3.endpoint", config.getS3Endpoint().orElseThrow(() -> new IllegalStateException("s3 endpoint is not present")).toString())
                    .buildOrThrow());
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
