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

import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import io.trino.Session;
import io.trino.client.QueryData;
import io.trino.client.QueryDataEncodings;

import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;

import static java.util.Objects.requireNonNull;

public final class EncodedQueryDataProducer
        implements QueryDataProducer
{
    private final Set<QueryDataEncoder> encoders;

    @Inject
    public EncodedQueryDataProducer(Set<QueryDataEncoder> encoders)
    {
        this.encoders = ImmutableSet.copyOf(requireNonNull(encoders, "encoders is null"));
    }

    @Override
    public QueryData produce(Session session, QueryResultRows rows, boolean completed, Consumer<Throwable> serializationExceptionHandler)
    {
        QueryDataEncodings supportedEncodings = session.getQueryDataEncoding()
                .map(QueryDataEncodings::parseEncodings)
                .orElseThrow(() -> new IllegalArgumentException("Expected query data formats to be non-empty list"));

        QueryDataEncoder encoder = select(supportedEncodings).orElseThrow(() ->
                new IllegalStateException("Could not find encoder for client supported query data encodings '%s'".formatted(session.getQueryDataEncoding())));
        return encoder.encode(session, rows, completed, serializationExceptionHandler);
    }

    private Optional<QueryDataEncoder> select(QueryDataEncodings supportedEncodings)
    {
        for (QueryDataEncoder serializer : encoders) {
            if (serializer.produces().enforceSingleEncoding().matchesAny(supportedEncodings)) {
                return Optional.of(serializer);
            }
        }

        return Optional.empty();
    }
}
