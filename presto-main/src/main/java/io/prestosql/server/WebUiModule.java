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
package io.prestosql.server;

import com.google.inject.Binder;
import com.google.inject.Scopes;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.prestosql.server.ui.ClusterResource;
import io.prestosql.server.ui.ClusterStatsResource;
import io.prestosql.server.ui.DisabledWebUiAuthenticationManager;
import io.prestosql.server.ui.FormWebUiAuthenticationManager;
import io.prestosql.server.ui.UiQueryResource;
import io.prestosql.server.ui.WebUiAuthenticationManager;
import io.prestosql.server.ui.WebUiConfig;

import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.airlift.http.server.HttpServerBinder.httpServerBinder;
import static io.airlift.jaxrs.JaxrsBinder.jaxrsBinder;

public class WebUiModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        httpServerBinder(binder).bindResource("/ui", "webapp").withWelcomeFile("index.html");

        configBinder(binder).bindConfig(WebUiConfig.class);

        if (buildConfigObject(WebUiConfig.class).isEnabled()) {
            binder.bind(WebUiAuthenticationManager.class).to(FormWebUiAuthenticationManager.class).in(Scopes.SINGLETON);
            jaxrsBinder(binder).bind(ClusterResource.class);
            jaxrsBinder(binder).bind(ClusterStatsResource.class);
            jaxrsBinder(binder).bind(UiQueryResource.class);
        }
        else {
            binder.bind(WebUiAuthenticationManager.class).to(DisabledWebUiAuthenticationManager.class).in(Scopes.SINGLETON);
        }
    }
}
