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
package io.trino.server.protocol.data;

import io.trino.Session;
import io.trino.client.ClientCapabilities;
import io.trino.client.JsonInlineQueryData;
import io.trino.client.QueryData;
import io.trino.server.protocol.JsonArrayResultsIterator;
import io.trino.server.protocol.QueryResultRows;

import java.util.List;
import java.util.function.Consumer;

import static io.trino.client.NoQueryData.NO_DATA;

/**
 * This QueryDataProducer that represents existing protocol semantics.
 * Streams partial result as JSON-serialized list of lists of objects on every available set of pages.
 */
public class InlineJsonQueryDataProducer
        implements QueryDataProducer
{
    @Override
    public QueryData produce(Session session, QueryResultRows rows, boolean completed, Consumer<Throwable> serializationExceptionHandler)
    {
        if (rows.isEmpty()) {
            return NO_DATA;
        }

        // We are passing whether data is present as we don't want to peek into iterable
        return JsonInlineQueryData.create(toIterableList(session, rows, serializationExceptionHandler), true);
    }

    @Override
    public Class<? extends QueryData> produces()
    {
        return JsonInlineQueryData.class;
    }

    private static Iterable<List<Object>> toIterableList(Session session, QueryResultRows rows, Consumer<Throwable> serializationExceptionHandler)
    {
        return new JsonArrayResultsIterator(
                session.toConnectorSession(),
                rows.getPages(),
                rows.getColumnsAndTypes().orElseThrow(),
                session.getClientCapabilities().contains(ClientCapabilities.PARAMETRIC_DATETIME.toString()),
                serializationExceptionHandler);
    }
}
