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

import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class PreferredQueryDataEncoderSelector
        implements QueryDataEncoder.EncoderSelector
{
    private final Logger log = Logger.get(PreferredQueryDataEncoderSelector.class);
    private final QueryDataEncoders encoders;
    private final SpoolingManagerRegistry spoolingManagerRegistry;

    @Inject
    public PreferredQueryDataEncoderSelector(QueryDataEncoders encoders, SpoolingManagerRegistry spoolingManagerRegistry)
    {
        this.encoders = requireNonNull(encoders, "encoders is null");
        this.spoolingManagerRegistry = requireNonNull(spoolingManagerRegistry, "spoolingManagerRegistry is null");
    }

    @Override
    public Optional<QueryDataEncoder.Factory> select(List<String> encodingIds)
    {
        if (spoolingManagerRegistry.getSpoolingManager().isEmpty()) {
            log.debug("Client requested one of the spooled encodings '%s' but spooling is disabled", encodingIds);
            return Optional.empty();
        }

        for (String encodingId : encodingIds) {
            if (encoders.exists(encodingId)) {
                return Optional.of(encoders.get(encodingId));
            }
        }
        log.warn("Requested one of the spooled encodings: %s, but these are only available: %s", encodingIds, encoders.getAvailableEncodings());
        return Optional.empty();
    }
}
