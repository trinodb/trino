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

import java.sql.SQLException;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class TrinoSqlException
        extends SQLException
{
    private final String queryId;
    private final String errorMessage;
    private final Optional<ErrorLocation> errorLocation;

    TrinoSqlException(String message, String sqlState, int errorCode, Throwable cause, String queryId, String errorMessage, Optional<ErrorLocation> errorLocation)
    {
        super(message, sqlState, errorCode, cause);
        this.queryId = requireNonNull(queryId, "queryId is null");
        this.errorMessage = requireNonNull(errorMessage, "errorMessage is null");
        this.errorLocation = requireNonNull(errorLocation, "errorLocation is null");
    }

    public String getQueryId()
    {
        return queryId;
    }

    /**
     * Returns the error message reported by the server, without the query ID decoration of {@link #getMessage()}.
     */
    public String getErrorMessage()
    {
        return errorMessage;
    }

    /**
     * Returns the position in the query text the error refers to, if the server reported one.
     */
    public Optional<ErrorLocation> getErrorLocation()
    {
        return errorLocation;
    }

    public static final class ErrorLocation
    {
        private final int lineNumber;
        private final int columnNumber;

        ErrorLocation(int lineNumber, int columnNumber)
        {
            this.lineNumber = lineNumber;
            this.columnNumber = columnNumber;
        }

        public int getLineNumber()
        {
            return lineNumber;
        }

        public int getColumnNumber()
        {
            return columnNumber;
        }
    }
}
