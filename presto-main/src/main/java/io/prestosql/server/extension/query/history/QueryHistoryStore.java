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
package io.prestosql.server.extension.query.history;

import io.prestosql.execution.QueryInfo;
import io.prestosql.server.extension.Extension;
import io.prestosql.spi.QueryId;

import java.io.Closeable;

/**
 * Extension for providing a richer query history.
 */
public interface QueryHistoryStore
        extends Extension, Closeable
{
    String getFullQueryInfo(QueryId queryId);

    String getBasicQueryInfo(QueryId queryId);

    void saveFullQueryInfo(QueryInfo queryInfo);
}
