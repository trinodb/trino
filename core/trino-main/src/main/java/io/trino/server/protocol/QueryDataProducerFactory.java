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

import io.trino.Session;
import io.trino.server.protocol.spooling.SpoolingQueryDataProducer;
import io.trino.spi.type.Type;

import java.util.List;

/**
 * Chooses how a query's result pages are turned into the {@link io.trino.client.QueryData} that a
 * client protocol expects. The output columns are only known once the query starts producing
 * results, so the choice is made then rather than at dispatch time.
 */
public interface QueryDataProducerFactory
{
    QueryDataProducer create(Session session, List<String> columnNames, List<Type> columnTypes);

    /**
     * Spooled segments when the client negotiated an encoding, inline JSON otherwise. Note that
     * the engine clears the encoding for statements it does not spool, so an empty encoding here
     * does not mean the client failed to ask for one.
     */
    QueryDataProducerFactory DEFAULT = (session, _, columnTypes) -> {
        if (session.getQueryDataEncoding().isEmpty()) {
            return new JsonBytesQueryDataProducer(session, columnTypes);
        }
        return new SpoolingQueryDataProducer(session.getQueryDataEncoding().orElseThrow());
    };
}
