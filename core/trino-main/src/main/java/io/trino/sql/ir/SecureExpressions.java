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
package io.trino.sql.ir;

import io.airlift.log.Logger;
import io.trino.spi.TrinoException;

import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.sql.ir.IrUtils.preOrder;
import static io.trino.sql.ir.SecureExpression.REDACTED;
import static java.util.Objects.requireNonNull;

public final class SecureExpressions
{
    private static final Logger log = Logger.get(SecureExpressions.class);

    private SecureExpressions() {}

    public static boolean isPresent(Expression expression)
    {
        return preOrder(expression).anyMatch(SecureExpression.class::isInstance);
    }

    /**
     * Replaces a failure of a secure expression with one carrying only its error code and location. The cause is
     * logged rather than attached, since causes are serialized into the failure info.
     */
    public static TrinoException redactFailure(RuntimeException failure)
    {
        requireNonNull(failure, "failure is null");
        log.debug(failure, "Secure expression failed: %s", failure.getMessage());
        if (failure instanceof TrinoException trinoException) {
            return new TrinoException(trinoException::getErrorCode, trinoException.getLocation(), REDACTED, null);
        }
        return new TrinoException(GENERIC_INTERNAL_ERROR, REDACTED);
    }
}
