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
package io.prestosql.plugin.session.db;

import com.google.common.collect.ImmutableMap;
import io.prestosql.plugin.session.SessionMatchSpec;
import io.prestosql.spi.session.SessionConfigurationContext;
import io.prestosql.spi.session.SessionPropertyConfigurationManager;

import javax.inject.Inject;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;

/**
 * A {@link SessionPropertyConfigurationManager} implementation that connects to a database for fetching information
 * about session property overrides given {@link SessionConfigurationContext}.
 */
public class DbSessionPropertyManager
        implements SessionPropertyConfigurationManager
{
    private final DbSpecsProvider specsProvider;

    @Inject
    public DbSessionPropertyManager(DbSpecsProvider specsProvider)
    {
        this.specsProvider = requireNonNull(specsProvider, "specsProvider is null");
    }

    @Override
    public Map<String, String> getSystemSessionProperties(SessionConfigurationContext context)
    {
        List<SessionMatchSpec> sessionMatchSpecs = specsProvider.get();

        // later properties override earlier properties
        Map<String, String> combinedProperties = new HashMap<>();
        for (SessionMatchSpec sessionMatchSpec : sessionMatchSpecs) {
            combinedProperties.putAll(sessionMatchSpec.match(context));
        }

        return ImmutableMap.copyOf(combinedProperties);
    }

    @Override
    public Map<String, Map<String, String>> getCatalogSessionProperties(SessionConfigurationContext context)
    {
        // NOT IMPLEMENTED YET
        return ImmutableMap.of();
    }
}
