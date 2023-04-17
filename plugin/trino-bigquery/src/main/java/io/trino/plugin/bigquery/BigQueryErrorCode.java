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

import io.trino.spi.ErrorCode;
import io.trino.spi.ErrorCodeSupplier;
import io.trino.spi.ErrorType;

import static io.trino.spi.ErrorType.EXTERNAL;
import static io.trino.spi.ErrorType.USER_ERROR;

public enum BigQueryErrorCode
        implements ErrorCodeSupplier
{
    BIGQUERY_VIEW_DESTINATION_TABLE_CREATION_FAILED(0, EXTERNAL),
    BIGQUERY_DATETIME_PARSING_ERROR(1, EXTERNAL),
    BIGQUERY_FAILED_TO_EXECUTE_QUERY(2, EXTERNAL),
    BIGQUERY_AMBIGUOUS_OBJECT_NAME(3, EXTERNAL),
    BIGQUERY_LISTING_DATASET_ERROR(4, EXTERNAL),
    BIGQUERY_UNSUPPORTED_OPERATION(5, USER_ERROR),
    BIGQUERY_INVALID_STATEMENT(6, USER_ERROR),
    BIGQUERY_PROXY_SSL_INITIALIZATION_FAILED(7, EXTERNAL),
    /**/;

    private final ErrorCode errorCode;

    BigQueryErrorCode(int code, ErrorType type)
    {
        errorCode = new ErrorCode(code + 0x0509_0000, name(), type);
    }

    @Override
    public ErrorCode toErrorCode()
    {
        return errorCode;
    }
}
