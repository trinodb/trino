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

import java.util.Map;

import static io.trino.plugin.varada.configuration.GlobalConfiguration.CONFIG_IS_SINGLE;

public interface VaradaBaseModule
        extends Module
{
    default VaradaBaseModule withConfig(Map<String, String> config)
    {
        return this;
    }

    default VaradaBaseModule withContext(ConnectorContext context)
    {
        return this;
    }

    static boolean isCoordinator(ConnectorContext context)
    {
        return context.getNodeManager().getCurrentNode().isCoordinator();
    }

    static boolean isWorker(ConnectorContext context, Map<String, String> config)
    {
        return isSingle(config) || !isCoordinator(context);
    }

    static boolean isSingle(Map<String, String> config)
    {
        return Boolean.parseBoolean(config.getOrDefault(CONFIG_IS_SINGLE, Boolean.FALSE.toString()));
    }

    default boolean shouldInstall()
    {
        return true;
    }
}
