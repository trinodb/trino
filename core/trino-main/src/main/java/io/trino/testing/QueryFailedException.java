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

public class QueryFailedException
        extends RuntimeException
{
    private final QueryId queryId;

    public QueryFailedException(QueryId queryId, String message)
    {
        super(message);
        this.queryId = queryId;
    }

    public QueryFailedException(QueryId queryId, String message, Throwable cause)
    {
        super(message, cause);
        this.queryId = queryId;
    }

    public QueryId getQueryId()
    {
        return queryId;
    }
}
