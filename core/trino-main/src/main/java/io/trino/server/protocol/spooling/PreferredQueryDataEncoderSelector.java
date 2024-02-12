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
import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;

public class PreferredQueryDataEncoderSelector
        implements QueryDataEncoderSelector
{
    private final Logger log = Logger.get(PreferredQueryDataEncoderSelector.class);

    private final Map<String, QueryDataEncoder.Factory> encoder;
    private final SpoolingManagerRegistry spoolingManagerRegistry;

    @Inject
    public PreferredQueryDataEncoderSelector(Set<QueryDataEncoder.Factory> factories, SpoolingManagerRegistry spoolingManagerRegistry)
    {
        this.encoder = requireNonNull(factories, "factories is null").stream()
                .collect(toImmutableMap(QueryDataEncoder.Factory::encodingId, identity()));
        this.spoolingManagerRegistry = requireNonNull(spoolingManagerRegistry, "spoolingManagerRegistry is null");
    }

    @Override
    public Optional<QueryDataEncoder.Factory> select(String encodingHeader)
    {
        if (spoolingManagerRegistry.getSpoolingManager().isEmpty()) {
            log.debug("Client requested spooled encoding '%s' but spooling is disabled", encodingHeader);
            return Optional.empty();
        }

        for (String encodingId : encodingHeader.split(",")) {
            QueryDataEncoder.Factory factory = encoder.get(encodingId);
            if (factory != null) {
                return Optional.of(factory);
            }
        }
        log.debug("None of the preferred spooled encodings `%s` are known and supported by the server", encodingHeader);
        return Optional.empty();
    }
}
