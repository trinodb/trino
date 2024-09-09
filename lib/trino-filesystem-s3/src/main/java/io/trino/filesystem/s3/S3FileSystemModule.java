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
package io.trino.filesystem.s3;

import com.google.common.base.VerifyException;
import com.google.inject.Binder;
import com.google.inject.BindingAnnotation;
import com.google.inject.Key;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.airlift.units.Duration;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.switching.SwitchingFileSystemFactory;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import java.util.function.Supplier;

import static com.google.inject.Scopes.SINGLETON;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.airlift.http.client.HttpClientBinder.httpClientBinder;
import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.RetentionPolicy.RUNTIME;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.weakref.jmx.guice.ExportBinder.newExporter;

public class S3FileSystemModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        configBinder(binder).bindConfig(S3FileSystemConfig.class);

        if (buildConfigObject(S3SecurityMappingEnabledConfig.class).isEnabled()) {
            install(new S3SecurityMappingModule());
        }
        else {
            binder.bind(TrinoFileSystemFactory.class).annotatedWith(FileSystemS3.class)
                    .to(S3FileSystemFactory.class).in(SINGLETON);
        }

        binder.bind(S3FileSystemStats.class).in(SINGLETON);
        newExporter(binder).export(S3FileSystemStats.class).withGeneratedName();
    }

    public static class S3SecurityMappingModule
            extends AbstractConfigurationAwareModule
    {
        @Override
        protected void setup(Binder binder)
        {
            S3SecurityMappingConfig config = buildConfigObject(S3SecurityMappingConfig.class);

            binder.bind(S3SecurityMappingProvider.class).in(SINGLETON);
            binder.bind(S3FileSystemLoader.class).in(SINGLETON);

            var mappingsBinder = binder.bind(new Key<Supplier<S3SecurityMappings>>() {});
            if (config.getConfigFile().isPresent()) {
                mappingsBinder.to(S3SecurityMappingsFileSource.class).in(SINGLETON);
            }
            else if (config.getConfigUri().isPresent()) {
                mappingsBinder.to(S3SecurityMappingsUriSource.class).in(SINGLETON);
                httpClientBinder(binder).bindHttpClient("s3-security-mapping", ForS3SecurityMapping.class)
                        .withConfigDefaults(httpConfig -> httpConfig
                                .setRequestTimeout(new Duration(10, SECONDS))
                                .setSelectorCount(1)
                                .setMinThreads(1));
            }
            else {
                throw new VerifyException("No security mapping source configured");
            }
        }

        @Provides
        @Singleton
        @FileSystemS3
        static TrinoFileSystemFactory createFileSystemFactory(S3FileSystemLoader loader)
        {
            return new SwitchingFileSystemFactory(loader);
        }
    }

    @Retention(RUNTIME)
    @Target({FIELD, PARAMETER, METHOD})
    @BindingAnnotation
    public @interface ForS3SecurityMapping {}
}
