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
package io.varada.cloudstorage;

import com.google.inject.Binder;
import com.google.inject.ConfigurationException;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.TypeLiteral;
import com.google.inject.multibindings.MapBinder;
import io.airlift.configuration.ConfigurationFactory;
import io.trino.filesystem.manager.FileSystemConfig;
import io.trino.spi.connector.ConnectorContext;
import io.varada.cloudstorage.azure.AzureCloudStorage;
import io.varada.cloudstorage.azure.AzureCloudStorageModule;
import io.varada.cloudstorage.gcs.GcsCloudStorage;
import io.varada.cloudstorage.gcs.GcsCloudStorageModule;
import io.varada.cloudstorage.hdfs.HdfsCloudStorage;
import io.varada.cloudstorage.hdfs.HdfsCloudStorageModule;
import io.varada.cloudstorage.s3.S3CloudStorage;
import io.varada.cloudstorage.s3.S3CloudStorageModule;

import java.lang.annotation.Annotation;
import java.util.Map;
import java.util.Optional;

import static com.google.inject.multibindings.MapBinder.newMapBinder;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static java.util.Objects.requireNonNull;

public class CloudStorageModule
        implements Module
{
    private final String catalogName;
    private final ConnectorContext context;
    private final ConfigurationFactory configurationFactory;
    private final Class<? extends Annotation> annotation;

    public CloudStorageModule(String catalogName,
                              ConnectorContext context,
                              ConfigurationFactory configurationFactory,
                              Class<? extends Annotation> annotation)
    {
        this.catalogName = requireNonNull(catalogName, "catalogName is null");
        this.context = requireNonNull(context, "context is null");
        this.configurationFactory = requireNonNull(configurationFactory, "configurationFactory is null");
        this.annotation = requireNonNull(annotation, "annotation is null");
    }

    @Override
    public void configure(Binder binder)
    {
        binder.bind(ConfigurationFactory.class).toInstance(configurationFactory);

        configBinder(binder).bindConfig(FileSystemConfig.class);
        FileSystemConfig config = configurationFactory.build(FileSystemConfig.class);

        Injector injector = Guice.createInjector(
                new HdfsCloudStorageModule(catalogName, context, configurationFactory, annotation, config.isHadoopEnabled()),
                new Module()
                {
                    @Override
                    public void configure(Binder binder)
                    {
                        MapBinder<String, CloudStorage> cloudStorageMap = newMapBinder(binder, String.class, CloudStorage.class, annotation);

                        if (config.isNativeS3Enabled()) {
                            binder.install(new S3CloudStorageModule(context, configurationFactory, annotation));
                            Key<S3CloudStorage> s3CloudStorageKey = Key.get(S3CloudStorage.class, annotation);
                            cloudStorageMap.addBinding("s3").to(s3CloudStorageKey);
                            cloudStorageMap.addBinding("s3a").to(s3CloudStorageKey);
                            cloudStorageMap.addBinding("s3n").to(s3CloudStorageKey);
                        }
                        if (config.isNativeAzureEnabled()) {
                            binder.install(new AzureCloudStorageModule(context, configurationFactory, annotation));
                            Key<AzureCloudStorage> azureCloudStorageKey = Key.get(AzureCloudStorage.class, annotation);
                            cloudStorageMap.addBinding("abfs").to(azureCloudStorageKey);
                            cloudStorageMap.addBinding("abfss").to(azureCloudStorageKey);
                        }
                        if (config.isNativeGcsEnabled()) {
                            binder.install(new GcsCloudStorageModule(context, configurationFactory, annotation));
                            Key<GcsCloudStorage> gcsCloudStorageKey = Key.get(GcsCloudStorage.class, annotation);
                            cloudStorageMap.addBinding("gs").to(gcsCloudStorageKey);
                        }
                    }
                });

        HdfsCloudStorage hdfsCloudStorage = null;
        try {
            hdfsCloudStorage = injector.getInstance(Key.get(HdfsCloudStorage.class, annotation));
        }
        catch (ConfigurationException ignored) {
            // do nothing
        }
        Map<String, CloudStorage> cloudStorageMap = injector.getInstance(Key.get(new TypeLiteral<>(){}, annotation));
        SwitchingCloudStorage switchingCloudStorage = new SwitchingCloudStorage(Optional.ofNullable(hdfsCloudStorage), cloudStorageMap);
        binder.bind(CloudStorage.class).annotatedWith(annotation).toInstance(switchingCloudStorage);
    }
}
