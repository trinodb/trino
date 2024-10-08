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
package io.trino.server.protocol.spooling.encoding;

import com.google.inject.Binder;
import com.google.inject.Scopes;
import com.google.inject.multibindings.Multibinder;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.server.protocol.spooling.QueryDataEncoder;
import io.trino.server.protocol.spooling.QueryDataEncoders;

import static com.google.inject.multibindings.Multibinder.newSetBinder;

public class QueryDataEncodingModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        Multibinder<QueryDataEncoder.Factory> encoderFactories = newSetBinder(binder, QueryDataEncoder.Factory.class);

        // json + compressed variants
        encoderFactories.addBinding().to(JsonQueryDataEncoder.Factory.class).in(Scopes.SINGLETON);
        encoderFactories.addBinding().to(JsonQueryDataEncoder.ZstdFactory.class).in(Scopes.SINGLETON);
        encoderFactories.addBinding().to(JsonQueryDataEncoder.Lz4Factory.class).in(Scopes.SINGLETON);

        binder.bind(QueryDataEncoders.class).in(Scopes.SINGLETON);
    }
}
