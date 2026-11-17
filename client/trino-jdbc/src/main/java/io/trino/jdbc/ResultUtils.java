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
package io.trino.jdbc;

import io.trino.client.QueryError;
import io.trino.client.QueryStatusInfo;
import io.trino.jdbc.TrinoSqlException.ErrorLocation;

import java.util.Optional;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

class ResultUtils
{
    private ResultUtils() {}

    static TrinoSqlException resultsException(QueryStatusInfo results)
    {
        QueryError error = requireNonNull(results.getError());
        String message = format("Query failed (#%s): %s", results.getId(), error.getMessage());
        Throwable cause = (error.getFailureInfo() == null) ? null : error.getFailureInfo().toException();
        Optional<ErrorLocation> errorLocation = Optional.ofNullable(error.getErrorLocation())
                .map(location -> new ErrorLocation(location.getLineNumber(), location.getColumnNumber()));
        return new TrinoSqlException(message, error.getSqlState(), error.getErrorCode(), cause, results.getId(), error.getMessage(), errorLocation);
    }
}
