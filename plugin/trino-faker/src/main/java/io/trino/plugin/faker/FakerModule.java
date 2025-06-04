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

package io.trino.plugin.faker;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;
import io.trino.spi.NodeManager;
import io.trino.spi.type.TypeManager;
import jakarta.inject.Inject;

import static io.airlift.configuration.ConfigBinder.configBinder;
import static java.util.Objects.requireNonNull;

public class FakerModule
        implements Module
{
    private final NodeManager nodeManager;
    private final TypeManager typeManager;

    @Inject
    public FakerModule(NodeManager nodeManager, TypeManager typeManager)
    {
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
    }

    @Override
    public void configure(Binder binder)
    {
        binder.bind(NodeManager.class).toInstance(nodeManager);
        binder.bind(TypeManager.class).toInstance(typeManager);

        binder.bind(FakerConnector.class).in(Scopes.SINGLETON);
        binder.bind(FakerMetadata.class).in(Scopes.SINGLETON);
        binder.bind(FakerSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(FakerPageSourceProvider.class).in(Scopes.SINGLETON);
        binder.bind(FakerPageSinkProvider.class).in(Scopes.SINGLETON);
        binder.bind(FakerFunctionProvider.class).in(Scopes.SINGLETON);
        configBinder(binder).bindConfig(FakerConfig.class);
    }
}
