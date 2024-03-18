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
package io.trino.plugin.varada.metrics;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import io.varada.tools.CatalogNameProvider;

public class MetricsModule
        implements Module
{
    private final String catalogName;

    public MetricsModule(String catalogName)
    {
        this.catalogName = catalogName;
    }

    @Override
    public void configure(Binder binder)
    {
        binder.bind(MetricsRegistry.class);
    }

    @SuppressWarnings("unused")
    @Provides
    @Singleton
    public CatalogNameProvider provideCatalogName()
    {
        return new CatalogNameProvider(catalogName);
    }
}
