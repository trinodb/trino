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
package io.trino.plugin.pinot.deepstore.gcs;

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

import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.airlift.configuration.ConfigBinder.configBinder;

public class PinotGcsModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        configBinder(binder).bindConfig(PinotGcsConfig.class);
        configBinder(binder).bindConfig(PinotDeepStoreConfig.class);
        newOptionalBinder(binder, DeepStore.class).setBinding().to(PinotDeepStore.class).in(Scopes.SINGLETON);
    }

    @Provides
    @Singleton
    public static PinotConfiguration getPinotFsConfiguration(PinotGcsConfig config)
    {
        return new PinotConfiguration(
                ImmutableMap.<String, Object>builder()
                        .put("storage.factory.class.gs", "org.apache.pinot.plugin.filesystem.GcsPinotFS")
                        .put("storage.factory.gs.projectId", config.getGcpProjectId().orElseThrow(() -> new IllegalStateException("gcp project id is not present")))
                        .put("storage.factory.gs.gcpKey", config.getGcpKey().orElseThrow(() -> new IllegalStateException("gcp key is not present")))
                        .buildOrThrow());
    }
}
