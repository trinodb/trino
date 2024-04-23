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
package io.trino.plugin.neo4j;

import io.trino.spi.ErrorCode;
import io.trino.spi.ErrorCodeSupplier;
import io.trino.spi.ErrorType;

import static io.trino.spi.ErrorType.EXTERNAL;

public enum Neo4jErrorCode
        implements ErrorCodeSupplier
{
    NEO4J_AMBIGUOUS_PROPERTY_TYPE(0, EXTERNAL), // schema issues
    NEO4J_UNSUPPORTED_PROPERTY_TYPE(1, EXTERNAL),
    NEO4J_UNKNOWN_TYPE(2, EXTERNAL);

    /**
     * Connectors can use error codes starting at the range 0x0100_0000
     * See https://github.com/trinodb/trino/wiki/Error-Codes
     *
     * @see io.trino.spi.StandardErrorCode
     */
    private final ErrorCode errorCode;

    Neo4jErrorCode(int code, ErrorType type)
    {
        errorCode = new ErrorCode(code + 0x0511_0000, name(), type);
    }

    @Override
    public ErrorCode toErrorCode()
    {
        return errorCode;
    }
}
