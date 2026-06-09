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
package io.trino.plugin.ai.functions;

import com.google.inject.Binder;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.function.FunctionBundle;
import io.trino.spi.function.FunctionBundleFactory;

import static java.util.Objects.requireNonNull;

public class AiModule
        extends AbstractConfigurationAwareModule
{
    private final FunctionBundleFactory functionBundleFactory;

    public AiModule(FunctionBundleFactory functionBundleFactory)
    {
        this.functionBundleFactory = requireNonNull(functionBundleFactory, "functionBundleFactory is null");
    }

    @Override
    protected void setup(Binder binder)
    {
        binder.bind(AiConnector.class).in(Scopes.SINGLETON);
        binder.bind(AiMetadata.class).in(Scopes.SINGLETON);
        binder.bind(AiFunctions.class).in(Scopes.SINGLETON);

        binder.bind(Connector.class).to(AiConnector.class).in(Scopes.SINGLETON);
        binder.bind(ConnectorMetadata.class).to(AiMetadata.class).in(Scopes.SINGLETON);

        install(switch (buildConfigObject(AiConfig.class).getProvider()) {
            case ANTHROPIC -> new AnthropicModule();
            case OPENAI -> new OpenAiModule();
        });
    }

    @Provides
    @Singleton
    public FunctionBundle getFunctionBundle(AiFunctions functions)
    {
        return functionBundleFactory.builder()
                .functions(functions)
                .build();
    }
}
