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
import io.trino.client.QueryData;
import io.trino.server.protocol.QueryResultRows;

import java.util.function.Consumer;

/**
 * QueryDataProducer is responsible for serializing result data according to a given format and returning it as {@link io.trino.client.QueryResults#data data field}.
 */
public interface QueryDataProducer
{
    QueryData produce(Session session, QueryResultRows rows, boolean completed, Consumer<Throwable> serializationExceptionHandler);

    Class<? extends QueryData> produces();
}
