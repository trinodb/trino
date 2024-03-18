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
package io.trino.plugin.varada.dispatcher;

import com.google.inject.Module;
import io.trino.plugin.varada.di.InitializationModule;
import io.trino.spi.cache.CacheManager;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorContext;
import io.varada.tools.util.Pair;

import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.google.common.base.Throwables.throwIfUnchecked;

@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
public class DispatcherConnectorFactory
{
    public static final String DISPATCHER_CONNECTOR_NAME = "warp_speed";
    private final Module storageEngineModule;
    private final Module amazonModule;
    private CacheManager cacheManager;

    public DispatcherConnectorFactory(Module storageEngineModule, Module amazonModule)
    {
        this.storageEngineModule = storageEngineModule;
        this.amazonModule = amazonModule;
    }

    @SuppressWarnings("removal")
    public Connector create(
            String catalogName,
            Map<String, String> config,
            ConnectorContext context,
            Optional<List<Class<? extends InitializationModule>>> optionalModules,
            Map<String, String> proxiedConnectorInitializerMap)
    {
        ClassLoader classLoader = this.getClass().getClassLoader();
        try {
            // use the class instance from InternalDispatcherConnectorFactory's classloader
            Class<?> moduleClass = classLoader.loadClass(Module.class.getName());

            Optional<List<Object>> optionalModuleInstances =
                    optionalModules.map(classes -> classes.stream()
                            .map(aClass -> {
                                try {
                                    Class<?> initModuleClass = classLoader.loadClass(aClass.getName());
                                    return InitializationModule.invokeCreateModule(initModuleClass,
                                            config,
                                            context,
                                            catalogName);
                                }
                                catch (ClassNotFoundException e) {
                                    throw new RuntimeException(e);
                                }
                            }).toList());
            Pair<Connector, CacheManager> result = (Pair<Connector, CacheManager>) classLoader.loadClass(InternalDispatcherConnectorFactory.class.getName())
                    .getMethod("createConnector",
                            String.class,
                            Map.class,
                            Optional.class,
                            Map.class,
                            moduleClass,
                            moduleClass,
                            ConnectorContext.class)
                    .invoke(null,
                            catalogName,
                            config,
                            optionalModuleInstances,
                            proxiedConnectorInitializerMap.entrySet()
                                    .stream()
                                    .collect(Collectors.toMap(Map.Entry::getKey, entry -> {
                                        try {
                                            return classLoader.loadClass(entry.getValue()).getDeclaredConstructor().newInstance();
                                        }
                                        catch (ClassNotFoundException | InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
                                            throw new RuntimeException(e);
                                        }
                                    })),
                            storageEngineModule,
                            amazonModule,
                            context);

            this.cacheManager = result.getRight();
            return result.getKey();
        }
        catch (InvocationTargetException e) {
            Throwable targetException = e.getTargetException();
            throwIfUnchecked(targetException);
            throw new RuntimeException(targetException);
        }
        catch (ReflectiveOperationException e) {
            throw new RuntimeException(e);
        }
    }

    public CacheManager getCacheManager()
    {
        return cacheManager;
    }
}
