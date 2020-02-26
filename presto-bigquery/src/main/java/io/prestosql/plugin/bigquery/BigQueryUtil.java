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
package io.prestosql.plugin.bigquery;

import com.google.cloud.bigquery.BigQueryError;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.TableId;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;

import java.util.Optional;

import static com.google.cloud.http.BaseHttpServiceException.UNKNOWN_CODE;
import static com.google.common.base.Throwables.getCausalChain;
import static java.lang.String.format;
import static java.util.stream.Collectors.joining;

class BigQueryUtil
{
    static final ImmutableSet<String> INTERNAL_ERROR_MESSAGES = ImmutableSet.of(
            "HTTP/2 error code: INTERNAL_ERROR",
            "Connection closed with unknown cause",
            "Received unexpected EOS on DATA frame from server");

    private BigQueryUtil() {}

    static boolean isRetryable(Throwable cause)
    {
        return getCausalChain(cause).stream().anyMatch(BigQueryUtil::isRetryableInternalError);
    }

    static boolean isRetryableInternalError(Throwable t)
    {
        if (t instanceof StatusRuntimeException) {
            StatusRuntimeException statusRuntimeException = (StatusRuntimeException) t;
            return statusRuntimeException.getStatus().getCode() == Status.Code.INTERNAL &&
                    INTERNAL_ERROR_MESSAGES.stream()
                            .anyMatch(message -> statusRuntimeException.getMessage().contains(message));
        }
        return false;
    }

    static BigQueryException convertToBigQueryException(BigQueryError error)
    {
        return new BigQueryException(UNKNOWN_CODE, error.getMessage(), error);
    }

    static String createSql(TableId table, ImmutableList<String> requiredColumns, String[] filters)
    {
        String columns = requiredColumns.isEmpty() ? "*" :
                requiredColumns.stream().map(column -> format("`%s`", column)).collect(joining(","));

        String whereClause = createWhereClause(filters)
                .map(clause -> "WHERE " + clause)
                .orElse("");

        return createSql(table, columns, filters);
    }

    // assuming the SELECT part is properly formatted, can be used to call functions such as COUNT and SUM
    static String createSql(TableId table, String formatedQuery, String[] filters)
    {
        String tableName = formatted(table);

        String whereClause = createWhereClause(filters)
                .map(clause -> "WHERE " + clause)
                .orElse("");

        return format("SELECT %s FROM `%s` %s", formatedQuery, tableName, whereClause);
    }

    static String formatted(TableId table)
    {
        return format("%s.%s.%s", table.getProject(), table.getDataset(), table.getTable());
    }

    // return empty if no filters are used
    private static Optional<String> createWhereClause(String[] filters)
    {
        return Optional.empty();
    }
}
