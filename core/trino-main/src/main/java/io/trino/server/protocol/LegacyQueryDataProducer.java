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
import io.trino.client.LegacyQueryData;
import io.trino.client.QueryData;

import java.util.function.Consumer;

import static io.trino.server.protocol.JsonArrayResultsIterator.toIterableList;

/**
 * This QueryDataProducer that represents existing protocol semantics.
 * Streams partial result as JSON-serialized list of lists of objects on every available set of pages.
 */
public final class LegacyQueryDataProducer
        implements QueryDataProducer
{
    @Override
    public QueryData produce(Session session, QueryResultRows rows, boolean completed, Consumer<Throwable> serializationExceptionHandler)
    {
        if (rows.isEmpty()) {
            return null;
        }
        return LegacyQueryData.create(toIterableList(session, rows, serializationExceptionHandler));
    }
}
