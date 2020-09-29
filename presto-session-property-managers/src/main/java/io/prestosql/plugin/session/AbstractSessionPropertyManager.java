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
package io.prestosql.plugin.session;

import com.google.common.collect.ImmutableMap;
import io.prestosql.spi.session.SessionConfigurationContext;
import io.prestosql.spi.session.SessionPropertyConfigurationManager;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class AbstractSessionPropertyManager
        implements SessionPropertyConfigurationManager
{
    @Override
    public final Map<String, String> getSystemSessionProperties(SessionConfigurationContext context)
    {
        // later properties override earlier properties
        Map<String, String> combinedProperties = new HashMap<>();
        for (SessionMatchSpec sessionMatchSpec : getSessionMatchSpecs()) {
            combinedProperties.putAll(sessionMatchSpec.match(context));
        }
        return ImmutableMap.copyOf(combinedProperties);
    }

    @Override
    public final Map<String, Map<String, String>> getCatalogSessionProperties(SessionConfigurationContext context)
    {
        // NOT IMPLEMENTED YET
        return ImmutableMap.of();
    }

    protected abstract List<SessionMatchSpec> getSessionMatchSpecs();
}
