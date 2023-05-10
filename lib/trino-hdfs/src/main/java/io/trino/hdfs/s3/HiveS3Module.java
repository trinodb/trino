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
package io.trino.hdfs.s3;

import com.google.inject.Binder;
import com.google.inject.Scopes;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.airlift.units.Duration;
import io.trino.hdfs.ConfigurationInitializer;
import io.trino.hdfs.DynamicConfigurationProvider;
import io.trino.hdfs.rubix.RubixEnabledConfig;
import org.apache.hadoop.conf.Configuration;

import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.airlift.http.client.HttpClientBinder.httpClientBinder;
import static org.weakref.jmx.guice.ExportBinder.newExporter;

public class HiveS3Module
        extends AbstractConfigurationAwareModule
{
    public static final String EMR_FS_CLASS_NAME = "com.amazon.ws.emr.hadoop.fs.EmrFileSystem";

    @Override
    protected void setup(Binder binder)
    {
        S3FileSystemType type = buildConfigObject(HiveS3TypeConfig.class).getS3FileSystemType();
        switch (type) {
            case TRINO:
                bindSecurityMapping(binder);

                newSetBinder(binder, ConfigurationInitializer.class).addBinding().to(TrinoS3ConfigurationInitializer.class).in(Scopes.SINGLETON);
                configBinder(binder).bindConfig(HiveS3Config.class);

                binder.bind(TrinoS3FileSystemStats.class).toInstance(TrinoS3FileSystem.getFileSystemStats());
                newExporter(binder).export(TrinoS3FileSystemStats.class)
                        .as(generator -> generator.generatedNameOf(TrinoS3FileSystem.class));
                return;
            case EMRFS:
                validateEmrFsClass();
                newSetBinder(binder, ConfigurationInitializer.class).addBinding().to(EmrFsS3ConfigurationInitializer.class).in(Scopes.SINGLETON);
                return;
            case HADOOP_DEFAULT:
                // configuration is done using Hadoop configuration files
                return;
        }
        throw new RuntimeException("Unknown file system type: " + type);
    }

    private void bindSecurityMapping(Binder binder)
    {
        S3SecurityMappingConfig configuration = buildConfigObject(S3SecurityMappingConfig.class);
        if (configuration.getConfigFilePath().isEmpty()) {
            return;
        }

        if (isHttp(configuration)) {
            binder.bind(S3SecurityMappingsProvider.class).to(UriBasedS3SecurityMappingsProvider.class).in(Scopes.SINGLETON);
            httpClientBinder(binder).bindHttpClient("s3SecurityMapping", ForS3SecurityMapping.class)
                    .withConfigDefaults(config -> config
                            .setRequestTimeout(Duration.succinctDuration(10, TimeUnit.SECONDS))
                            .setSelectorCount(1)
                            .setMinThreads(1));
        }
        else {
            binder.bind(S3SecurityMappingsProvider.class).to(FileBasedS3SecurityMappingsProvider.class).in(Scopes.SINGLETON);
        }

        newSetBinder(binder, DynamicConfigurationProvider.class).addBinding()
                .to(S3SecurityMappingConfigurationProvider.class).in(Scopes.SINGLETON);

        checkArgument(!getProperty("hive.s3select-pushdown.enabled").map(Boolean::parseBoolean).orElse(false), "S3 security mapping is not compatible with S3 Select pushdown");
        checkArgument(!buildConfigObject(RubixEnabledConfig.class).isCacheEnabled(), "S3 security mapping is not compatible with Hive caching");
    }

    private static void validateEmrFsClass()
    {
        // verify that the class exists
        try {
            Class.forName(EMR_FS_CLASS_NAME);
        }
        catch (ClassNotFoundException e) {
            throw new RuntimeException("EMR File System class not found: " + EMR_FS_CLASS_NAME, e);
        }
    }

    private static boolean isHttp(S3SecurityMappingConfig config)
    {
        return config.getConfigFilePath().map(configFile -> configFile.startsWith("https://") || configFile.startsWith("http://")).orElse(false);
    }

    public static class EmrFsS3ConfigurationInitializer
            implements ConfigurationInitializer
    {
        @Override
        public void initializeConfiguration(Configuration config)
        {
            // re-map filesystem schemes to use the Amazon EMR file system
            config.set("fs.s3.impl", EMR_FS_CLASS_NAME);
            config.set("fs.s3a.impl", EMR_FS_CLASS_NAME);
            config.set("fs.s3n.impl", EMR_FS_CLASS_NAME);
        }
    }
}
