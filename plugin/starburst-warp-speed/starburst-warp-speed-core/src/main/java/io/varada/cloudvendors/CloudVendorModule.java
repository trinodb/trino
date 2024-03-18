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

import com.google.inject.Module;
import io.trino.spi.connector.ConnectorContext;
import io.varada.cloudvendors.configuration.CloudVendorConfiguration;
import io.varada.cloudvendors.configuration.StoreType;
import io.varada.cloudvendors.local.LocalStoreModule;

import java.lang.annotation.Annotation;
import java.util.Map;

public interface CloudVendorModule
        extends Module
{
    static CloudVendorModule getModule(
            ConnectorContext context,
            Class<? extends Annotation> annotation,
            String catalogName,
            Map<String, String> config)
    {
        return getModule(
                context,
                null,
                annotation,
                catalogName,
                config,
                CloudVendorConfiguration.STORE_PATH,
                CloudVendorConfiguration.STORE_TYPE,
                CloudVendorConfiguration.class);
    }

    static CloudVendorModule getModule(
            ConnectorContext context,
            String prefix,
            Class<? extends Annotation> annotation,
            String catalogName,
            Map<String, String> config,
            String storePathName,
            String storeTypeName,
            Class<? extends CloudVendorConfiguration> configurationClazz)
    {
        StoreType storeType = StoreType.ofConfigName(config.get(storePathName), config.get(storeTypeName));
        return switch (storeType) {
            case S3, AZURE, GS -> new CloudVendorStorageModule(annotation, prefix, config, catalogName, context, configurationClazz);
            case LOCAL -> new LocalStoreModule(prefix, annotation, config, configurationClazz);
        };
    }
}
