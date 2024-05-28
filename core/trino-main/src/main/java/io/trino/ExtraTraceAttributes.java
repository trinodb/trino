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
package io.trino;

import com.google.inject.Inject;
import io.trino.dispatcher.DispatchManager;
import io.trino.spi.QueryId;

import java.util.Collection;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

public class ExtraTraceAttributes
        implements ExtraTraceAttributesProvider
{
    public static final String CLIENT_TAGS = "clientTags";

    private final DispatchManager dispatchManager;

    @Inject
    public ExtraTraceAttributes(DispatchManager dispatchManager)
    {
        this.dispatchManager = requireNonNull(dispatchManager, "dispatchManager is null");
    }

    /**
     * @throws java.util.NoSuchElementException if the query is not found
     */
    @Override
    public Map<String, String> getExtraTraceAttributes(QueryId queryId)
    {
        return dispatchManager.getQueryInfo(queryId).getSession().getCustomTraceAttributes();
    }

    /**
     * @throws java.util.NoSuchElementException if the query is not found
     */
    public String getClientTags(QueryId queryId)
    {
        return getExtraTraceAttributes(queryId).get(CLIENT_TAGS);
    }

    public static String sortAndConsolidateForValue(Collection<String> vals)
    {
        PriorityQueue<String> valQueue = new PriorityQueue<>(vals);
        return valQueue.stream().collect(Collectors.joining(","));
    }
}
