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

import com.google.inject.Inject;
import io.trino.Session;
import io.trino.client.JsonQueryResults;

import java.util.List;
import java.util.Set;

import static com.google.common.base.Verify.verify;
import static java.util.Objects.requireNonNull;

public class DefaultResultSetProducerFactory
        implements ResultSetProducerFactory
{
    private final Set<ResultSetProducer> producers;

    @Inject
    public DefaultResultSetProducerFactory(Set<ResultSetProducer> producers)
    {
        this.producers = requireNonNull(producers, "producers is null");
    }

    @Override
    public ResultSetProducer create(Session session)
    {
        List<ResultSetProducer> jsonProducers = producers.stream()
                .filter(producer -> producer.produces().equals(JsonQueryResults.class))
                .toList();
        verify(jsonProducers.size() == 1, "There should be exactly single result set producer for json format");
        return jsonProducers.get(0);
    }
}
