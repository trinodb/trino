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
package io.trino.testing;

import io.trino.spi.QueryId;

import static java.util.Objects.requireNonNull;

public class MaterializedResultWithQueryId
{
    private final QueryId queryId;
    private final MaterializedResult result;

    public MaterializedResultWithQueryId(QueryId queryId, MaterializedResult result)
    {
        this.queryId = requireNonNull(queryId, "queryId is null");
        this.result = requireNonNull(result, "result is null");
    }

    public QueryId getQueryId()
    {
        return queryId;
    }

    public MaterializedResult getResult()
    {
        return result;
    }
}
