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
package io.trino.server.protocol;

import com.google.inject.Inject;
import io.trino.Session;
import io.trino.server.protocol.spooling.SpoolingConfig;
import io.trino.server.protocol.spooling.SpoolingConfig.SegmentRetrievalMode;
import io.trino.server.protocol.spooling.SpoolingQueryDataProducer;
import io.trino.spi.spool.SpoolingManager;
import io.trino.spi.type.Type;

import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class QueryDataProducerFactory
{
    private final Optional<SpoolingManager> spoolingManager;
    private final Optional<SegmentRetrievalMode> retrievalMode;

    @Inject
    public QueryDataProducerFactory(Optional<SpoolingManager> spoolingManager, Optional<SpoolingConfig> spoolingConfig)
    {
        this.spoolingManager = requireNonNull(spoolingManager, "spoolingManager is null");
        this.retrievalMode = requireNonNull(spoolingConfig, "spoolingConfig is null").map(SpoolingConfig::getRetrievalMode);
    }

    public QueryDataProducer create(Session session, List<Type> types)
    {
        if (session.getQueryDataEncoding().isEmpty()) {
            return new JsonBytesQueryDataProducer(session, types);
        }
        return new SpoolingQueryDataProducer(session.getQueryDataEncoding().orElseThrow(), spoolingManager.orElseThrow(), retrievalMode.orElseThrow());
    }
}
