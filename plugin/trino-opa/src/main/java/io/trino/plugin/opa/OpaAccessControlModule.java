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
package io.trino.plugin.opa;

import com.google.inject.Binder;
import com.google.inject.Scopes;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.plugin.opa.schema.OpaBatchQueryResult;
import io.trino.spi.security.SystemAccessControl;

import static io.airlift.configuration.ConditionalModule.conditionalModule;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.airlift.json.JsonCodecBinder.jsonCodecBinder;

public class OpaAccessControlModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        configBinder(binder).bindConfig(OpaConfig.class);
        install(conditionalModule(
                OpaConfig.class,
                config -> config.getOpaBatchUri().isPresent(),
                new OpaBatchAccessControlModule(),
                new OpaSingleAuthorizerModule()));
    }

    public static class OpaSingleAuthorizerModule
            extends AbstractConfigurationAwareModule
    {
        @Override
        protected void setup(Binder binder)
        {
            binder.bind(SystemAccessControl.class).to(OpaAccessControl.class).in(Scopes.SINGLETON);
        }
    }

    public static class OpaBatchAccessControlModule
            extends AbstractConfigurationAwareModule
    {
        @Override
        protected void setup(Binder binder)
        {
            jsonCodecBinder(binder).bindJsonCodec(OpaBatchQueryResult.class);
            binder.bind(SystemAccessControl.class).to(OpaBatchAccessControl.class).in(Scopes.SINGLETON);
        }
    }
}
