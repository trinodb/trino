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
package io.trino.plugin.bigquery;

import com.google.cloud.bigquery.BigQueryError;
import com.google.cloud.bigquery.BigQueryException;
import com.google.common.collect.ImmutableSet;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;

import java.util.Arrays;
import java.util.Optional;
import java.util.Set;

import static com.google.cloud.http.BaseHttpServiceException.UNKNOWN_CODE;
import static com.google.common.base.Throwables.getCausalChain;

public final class BigQueryUtil
{
    private static final Set<String> INTERNAL_ERROR_MESSAGES = ImmutableSet.of(
            "HTTP/2 error code: INTERNAL_ERROR",
            "Connection closed with unknown cause",
            "Received unexpected EOS on DATA frame from server");

    private BigQueryUtil() {}

    public static boolean isRetryable(Throwable cause)
    {
        return getCausalChain(cause).stream().anyMatch(BigQueryUtil::isRetryableInternalError);
    }

    private static boolean isRetryableInternalError(Throwable t)
    {
        if (t instanceof StatusRuntimeException) {
            StatusRuntimeException statusRuntimeException = (StatusRuntimeException) t;
            return statusRuntimeException.getStatus().getCode() == Status.Code.INTERNAL &&
                    INTERNAL_ERROR_MESSAGES.stream()
                            .anyMatch(message -> statusRuntimeException.getMessage().contains(message));
        }
        return false;
    }

    public static BigQueryException convertToBigQueryException(BigQueryError error)
    {
        return new BigQueryException(UNKNOWN_CODE, error.getMessage(), error);
    }

    public static String toBigQueryColumnName(String columnName)
    {
        Optional<BigQueryPseudoColumn> pseudoColumn = Arrays.stream(BigQueryPseudoColumn.values())
                .filter(column -> column.getTrinoColumnName().equals(columnName))
                .findFirst();
        if (pseudoColumn.isPresent()) {
            return pseudoColumn.get().getBigqueryColumnName();
        }
        return columnName;
    }
}
