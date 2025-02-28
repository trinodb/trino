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

package io.trino.plugin.influxdb;

import com.google.inject.Binder;
import com.google.inject.Module;

import static com.google.inject.Scopes.SINGLETON;
import static io.airlift.configuration.ConfigBinder.configBinder;

public class InfluxModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        configBinder(binder).bindConfig(InfluxConfig.class);

        binder.bind(InfluxClient.class).to(NativeInfluxClient.class).in(SINGLETON);
        binder.bind(NativeInfluxClient.class).in(SINGLETON);
        binder.bind(InfluxMetadata.class).in(SINGLETON);
        binder.bind(InfluxSplitManager.class).in(SINGLETON);
        binder.bind(InfluxRecordSetProvider.class).in(SINGLETON);
        binder.bind(InfluxConnector.class).in(SINGLETON);
    }
}
