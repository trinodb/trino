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
package io.trino.plugin.session;

import com.google.common.collect.Table;
import io.trino.spi.session.SessionConfigurationContext;
import io.trino.spi.session.SessionPropertyConfigurationManager;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableTable.toImmutableTable;

public abstract class AbstractSessionPropertyManager
        implements SessionPropertyConfigurationManager
{
    @Override
    public final Map<String, String> getSystemSessionProperties(SessionConfigurationContext context)
    {
        Map<String, String> sessionProperties = getSessionProperties(context);
        return sessionProperties.entrySet().stream()
                .filter(property -> !isCatalogSessionProperty(property))
                .collect(toImmutableMap(Entry::getKey, Entry::getValue));
    }

    @Override
    public final Map<String, Map<String, String>> getCatalogSessionProperties(SessionConfigurationContext context)
    {
        Map<String, String> sessionProperties = getSessionProperties(context);
        Table<String, String, String> catalogsSessionProperties = sessionProperties.entrySet().stream()
                .filter(AbstractSessionPropertyManager::isCatalogSessionProperty)
                .collect(toImmutableTable(
                        catalogProperty -> catalogProperty.getKey().split("\\.", 2)[0],
                        catalogProperty -> catalogProperty.getKey().split("\\.", 2)[1],
                        Entry::getValue));
        return catalogsSessionProperties.rowMap();
    }

    protected abstract List<SessionMatchSpec> getSessionMatchSpecs();

    private Map<String, String> getSessionProperties(SessionConfigurationContext context)
    {
        // later properties override earlier properties
        Map<String, String> combinedProperties = new HashMap<>();
        for (SessionMatchSpec sessionMatchSpec : getSessionMatchSpecs()) {
            combinedProperties.putAll(sessionMatchSpec.match(context));
        }
        return combinedProperties;
    }

    private static boolean isCatalogSessionProperty(Entry<String, String> property)
    {
        return property.getKey().contains(".");
    }
}
