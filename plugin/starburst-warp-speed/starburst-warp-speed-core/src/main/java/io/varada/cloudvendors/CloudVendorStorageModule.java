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
package io.varada.cloudvendors;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import io.airlift.configuration.ConfigurationFactory;
import io.airlift.log.Logger;
import io.trino.spi.connector.ConnectorContext;
import io.varada.cloudstorage.CloudStorage;
import io.varada.cloudstorage.CloudStorageModule;
import io.varada.cloudvendors.configuration.CloudVendorConfiguration;

import java.lang.annotation.Annotation;
import java.util.Map;

import static java.util.Objects.requireNonNull;

public class CloudVendorStorageModule
        extends CloudVendorModulePublic
{
    private static final Logger logger = Logger.get(CloudVendorStorageModule.class);

    private final String catalogName;
    private final ConnectorContext context;

    public CloudVendorStorageModule(
            Class<? extends Annotation> annotation,
            String prefix,
            Map<String, String> config,
            String catalogName,
            ConnectorContext context,
            Class<? extends CloudVendorConfiguration> configurationClazz)
    {
        super(prefix, annotation, config, configurationClazz);
        this.catalogName = requireNonNull(catalogName, "catalogName is null");
        this.context = requireNonNull(context, "context is null");
    }

    @Override
    public void configure()
    {
        super.configure();

        ConfigurationFactory configurationFactory = new ConfigurationFactoryWithPrefix(config, prefix, logger::warn);
        Injector injector = Guice.createInjector(new CloudStorageModule(catalogName, context, configurationFactory, annotation));

        CloudStorage cloudStorage = injector.getInstance(Key.get(CloudStorage.class, annotation));
        bind(CloudVendorService.class).annotatedWith(annotation).toInstance(new CloudVendorStorageService(cloudStorage));
    }
}
