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
package io.trino.plugin.prometheus;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;
import io.trino.plugin.base.session.SessionPropertiesProvider;

import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.airlift.json.JsonCodecBinder.jsonCodecBinder;

public class PrometheusModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        binder.bind(PrometheusConnector.class).in(Scopes.SINGLETON);
        binder.bind(PrometheusMetadata.class).in(Scopes.SINGLETON);
        binder.bind(PrometheusClient.class).in(Scopes.SINGLETON);
        binder.bind(PrometheusSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(PrometheusClock.class).in(Scopes.SINGLETON);
        binder.bind(PrometheusRecordSetProvider.class).in(Scopes.SINGLETON);
        binder.bind(PrometheusSessionProperties.class).in(Scopes.SINGLETON);
        configBinder(binder).bindConfig(PrometheusConnectorConfig.class);

        newSetBinder(binder, SessionPropertiesProvider.class).addBinding().to(PrometheusSessionProperties.class).in(Scopes.SINGLETON);

        jsonCodecBinder(binder).bindMapJsonCodec(String.class, Object.class);
    }
}
