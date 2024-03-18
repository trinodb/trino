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
package io.trino.plugin.varada.di;

import com.google.inject.Module;
import io.trino.spi.connector.ConnectorContext;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Map;

/**
 * SubClasses of this class must have a private constructor and implement a static method
 * <code>
 * public Object createModule(Class&lt;?&gt; clazz, Map&lt;String, String&gt; config,ConnectorContext context,String catalogName) {
 * return new XXX(config, context, catalogName);
 * }
 * </code>
 */
public interface InitializationModule
        extends VaradaBaseModule
{
    static Object invokeCreateModule(
            Class<?> clazz,
            Map<String, String> config,
            ConnectorContext context,
            String catalogName)
    {
        try {
            Constructor<?> constructor = clazz.getConstructor();
            Object initModule = constructor.newInstance();
            return clazz
                    .getMethod("createModule", Map.class, ConnectorContext.class, String.class)
                    .invoke(initModule, config, context, catalogName);
        }
        catch (IllegalAccessException | InvocationTargetException | NoSuchMethodException | InstantiationException e) {
            throw new RuntimeException(e);
        }
    }

    @SuppressWarnings("unused")
    Module createModule(Map<String, String> config, ConnectorContext connectorContext, String catalogName);
}
