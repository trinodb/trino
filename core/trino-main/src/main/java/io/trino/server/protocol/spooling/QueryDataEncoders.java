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
package io.trino.server.protocol.spooling;

import com.google.inject.Inject;
import io.airlift.log.Logger;

import java.util.Map;
import java.util.Set;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.util.Objects.requireNonNull;

public class QueryDataEncoders
{
    private static final Logger LOG = Logger.get(QueryDataEncoders.class);

    private final Map<String, QueryDataEncoder.Factory> factories;
    private final boolean enabled;

    @Inject
    public QueryDataEncoders(SpoolingEnabledConfig enabledConfig, Set<QueryDataEncoder.Factory> factories)
    {
        this.enabled = enabledConfig.isEnabled();
        this.factories = requireNonNull(factories, "factories is null")
                .stream()
                .map(factory -> Map.entry(factory.encoding(), factory))
                .collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));

        if (enabled) {
            LOG.info("Spooling protocol is enabled with encodings: %s", getAvailableEncodings());
        }
    }

    public boolean exists(String encoding)
    {
        if (!enabled) {
            throw new IllegalStateException("Spooling protocol is not enabled");
        }
        return factories.containsKey(encoding);
    }

    public QueryDataEncoder.Factory get(String encoding)
    {
        if (!enabled) {
            throw new IllegalStateException("Spooling protocol is not enabled");
        }
        if (!exists(encoding)) {
            throw new IllegalArgumentException("Unknown spooling protocol encoding: " + encoding);
        }

        return factories.get(encoding);
    }

    public Set<String> getAvailableEncodings()
    {
        if (!enabled) {
            throw new IllegalStateException("Spooling protocol is not enabled");
        }
        return factories.keySet();
    }
}
