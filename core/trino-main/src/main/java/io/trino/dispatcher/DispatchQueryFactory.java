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
package io.trino.dispatcher;

import io.trino.Session;
import io.trino.execution.QueryPreparer.PreparedQuery;
import io.trino.server.protocol.Slug;
import io.trino.spi.resourcegroups.ResourceGroupId;
import io.trino.sql.RedactedQuery;
import io.trino.transaction.TransactionId;

import java.util.Optional;
import java.util.function.Function;

public interface DispatchQueryFactory
{
    DispatchQuery createDispatchQuery(
            Session session,
            Optional<TransactionId> transactionId,
            Function<Session, RedactedQuery> queryProvider,
            PreparedQuery preparedQuery,
            Slug slug,
            ResourceGroupId resourceGroup);
}
