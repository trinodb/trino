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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import io.trino.Session;
import io.trino.client.EncodedQueryData;
import io.trino.client.QueryDataEncodings;

import java.util.List;
import java.util.function.Consumer;

import static io.trino.client.QueryDataPart.inlineQueryDataPart;
import static io.trino.client.QueryDataReference.INLINE;
import static io.trino.client.QueryDataSerialization.JSON;
import static io.trino.server.protocol.JsonArrayResultsIterator.toIterableList;
import static java.util.Objects.requireNonNull;

public class JsonQueryDataEncoder
        implements QueryDataEncoder
{
    private final ObjectMapper mapper;

    @Inject
    public JsonQueryDataEncoder(ObjectMapper mapper)
    {
        this.mapper = requireNonNull(mapper, "mapper is null");
    }

    @Override
    public EncodedQueryData encode(Session session, QueryResultRows resultRows, boolean endOfData, Consumer<Throwable> throwableConsumer)
    {
        if (resultRows.isEmpty()) {
            return null;
        }

        try {
            Iterable<List<Object>> iterable = toIterableList(session, resultRows, throwableConsumer);
            return EncodedQueryData.builder(QueryDataEncodings.singleEncoding(INLINE, JSON))
                    .addPart(inlineQueryDataPart(mapper.writeValueAsBytes(iterable), resultRows.getTotalRowsCount()))
                    .build();
        }
        catch (JsonProcessingException e) {
            throwableConsumer.accept(e);
            throw new RuntimeException("Could not serialize data to JSON", e);
        }
    }

    @Override
    public QueryDataEncodings produces()
    {
        return QueryDataEncodings.singleEncoding(INLINE, JSON);
    }
}
