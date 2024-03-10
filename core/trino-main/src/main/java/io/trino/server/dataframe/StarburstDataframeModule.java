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
package io.trino.server.dataframe;

import com.google.inject.Binder;
import com.google.inject.Scopes;
import com.starburstdata.dataframe.analyzer.AnalyzerFactory;
import com.starburstdata.dataframe.analyzer.LiteralMapper;
import com.starburstdata.dataframe.analyzer.TypeCoercion;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.FeaturesConfig;
import io.trino.server.ServerConfig;

import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.airlift.jaxrs.JaxrsBinder.jaxrsBinder;

public class StarburstDataframeModule
        extends AbstractConfigurationAwareModule
{
    @Override
    public void setup(Binder binder)
    {
        ServerConfig serverConfig = buildConfigObject(ServerConfig.class);
        configBinder(binder).bindConfig(FeaturesConfig.class);
        if (serverConfig.isDataframeApiEnabled() && serverConfig.isCoordinator()) {
            jaxrsBinder(binder).bind(StarburstDataframePlanResource.class);
            binder.bind(TestingTrinoMetadataFactory.class).in(Scopes.SINGLETON);
            binder.bind(AnalyzerFactory.class).in(Scopes.SINGLETON);
            binder.bind(DataTypeMapper.class).in(Scopes.SINGLETON);
            binder.bind(LiteralMapper.class).in(Scopes.SINGLETON);
            binder.bind(TypeCoercion.class).in(Scopes.SINGLETON);
        }
    }
}
