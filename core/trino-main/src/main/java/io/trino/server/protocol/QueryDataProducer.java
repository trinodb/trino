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

import io.trino.client.QueryData;
import io.trino.server.ExternalUriInfo;
import io.trino.spi.TrinoException;

import java.util.function.Consumer;

import static io.trino.spi.StandardErrorCode.SERIALIZATION_ERROR;

public interface QueryDataProducer
{
    QueryData produce(ExternalUriInfo uriInfo, QueryResultRows rows, Consumer<TrinoException> throwableConsumer);

    default void close()
    {
    }

    QueryDataProducer THROWING = (uriInfo, rows, throwableConsumer) -> {
        if (!rows.isEmpty()) {
            throwableConsumer.accept(new TrinoException(SERIALIZATION_ERROR, "Protocol violation: query data producer is not initialized"));
        }
        return QueryData.NULL;
    };
}
