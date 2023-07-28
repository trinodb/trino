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
package io.trino.server.protocol.resultset;

import io.trino.Session;
import io.trino.client.QueryResults;
import io.trino.client.QueryResultsFormat;
import io.trino.server.protocol.QueryResultRows;

import java.util.function.Consumer;

import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.base.Verify.verify;
import static java.util.Objects.requireNonNull;

/**
 * ResultSetProducer is responsible for serializing result data according to a given format and returning it with the associated QueryResults implementation.
 */
public interface ResultSetProducer
{
    QueryResults handleNextRows(Session session, QueryResults base, QueryResultRows rows, Consumer<Throwable> handleSerializationException);

    Class<? extends QueryResults> produces();

    default String formatName()
    {
        QueryResultsFormat annotation = produces().getAnnotation(QueryResultsFormat.class);
        requireNonNull(annotation, "@ResultSetFormat is missing on %s".formatted(produces()));
        verify(!isNullOrEmpty(annotation.formatName()), "ResultSetFormat.formatName is null or empty");
        return annotation.formatName();
    }
}
