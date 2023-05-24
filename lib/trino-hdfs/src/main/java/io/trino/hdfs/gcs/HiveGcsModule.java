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
package io.trino.hdfs.gcs;

import com.google.inject.Binder;
import com.google.inject.Scopes;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.hdfs.ConfigurationInitializer;
import io.trino.hdfs.DynamicConfigurationProvider;
import io.trino.hdfs.rubix.RubixEnabledConfig;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static io.airlift.configuration.ConfigBinder.configBinder;

public class HiveGcsModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        configBinder(binder).bindConfig(HiveGcsConfig.class);

        newSetBinder(binder, ConfigurationInitializer.class).addBinding().to(GoogleGcsConfigurationInitializer.class).in(Scopes.SINGLETON);

        if (buildConfigObject(HiveGcsConfig.class).isUseGcsAccessToken()) {
            checkArgument(!buildConfigObject(RubixEnabledConfig.class).isCacheEnabled(), "Use of GCS access token is not compatible with Hive caching");
            newSetBinder(binder, DynamicConfigurationProvider.class).addBinding().to(GcsConfigurationProvider.class).in(Scopes.SINGLETON);
        }
    }
}
