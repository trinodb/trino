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
package io.trino.plugin.lakehouse;

import com.google.inject.Binder;
import com.google.inject.Scopes;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.plugin.hudi.HudiConfig;
import io.trino.plugin.hudi.HudiExecutorModule;
import io.trino.plugin.hudi.HudiMetadataFactory;
import io.trino.plugin.hudi.HudiPageSourceProvider;
import io.trino.plugin.hudi.HudiSessionProperties;
import io.trino.plugin.hudi.HudiSplitManager;
import io.trino.plugin.hudi.HudiTableProperties;
import io.trino.plugin.hudi.HudiTransactionManager;

import static io.airlift.configuration.ConfigBinder.configBinder;

public class LakehouseHudiModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        configBinder(binder).bindConfig(HudiConfig.class);

        binder.bind(HudiPageSourceProvider.class).in(Scopes.SINGLETON);
        binder.bind(HudiSessionProperties.class).in(Scopes.SINGLETON);
        binder.bind(HudiSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(HudiTableProperties.class).in(Scopes.SINGLETON);

        binder.bind(HudiTransactionManager.class).in(Scopes.SINGLETON);
        binder.bind(HudiMetadataFactory.class).in(Scopes.SINGLETON);

        binder.install(new HudiExecutorModule());
    }
}
