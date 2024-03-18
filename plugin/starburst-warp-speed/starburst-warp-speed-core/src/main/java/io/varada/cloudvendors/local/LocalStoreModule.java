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
package io.varada.cloudvendors.local;

import io.varada.cloudvendors.CloudVendorModulePrivate;
import io.varada.cloudvendors.CloudVendorService;
import io.varada.cloudvendors.configuration.CloudVendorConfiguration;

import java.lang.annotation.Annotation;
import java.util.Map;

public class LocalStoreModule
        extends CloudVendorModulePrivate
{
    @SuppressWarnings("unused")
    public LocalStoreModule(
            String prefix,
            Class<? extends Annotation> annotation,
            Map<String, String> config,
            Class<? extends CloudVendorConfiguration> configurationClazz)
    {
        super(prefix, annotation, config, configurationClazz);
    }

    @Override
    public void configure()
    {
        super.configure();
        binder().bind(CloudVendorService.class).annotatedWith(annotation).to(LocalStoreService.class);
        expose(CloudVendorService.class).annotatedWith(annotation);
    }
}
