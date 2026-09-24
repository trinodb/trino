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

import com.google.common.collect.Sets;
import io.airlift.log.Logger;
import io.trino.spi.TrinoException;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

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
     * Replaces a failure of a secure expression with one carrying only its error code and location, since the message
     * and the cause can contain the values the expression protects. The failure is logged the same way.
     */
    public static TrinoException redactFailure(Exception failure)
    {
        requireNonNull(failure, "failure is null");
        if (failure instanceof InterruptedException) {
            Thread.currentThread().interrupt();
        }
        log.debug("Secure expression failed: %s", describeFailure(failure));
        if (failure instanceof TrinoException trinoException) {
            return new TrinoException(trinoException::getErrorCode, trinoException.getLocation(), REDACTED, null);
        }
        return new TrinoException(GENERIC_INTERNAL_ERROR, REDACTED);
    }

    /**
     * Describes a failure by its error code, location and exception classes, without the messages.
     */
    public static String describeFailure(Throwable failure)
    {
        List<String> classes = new ArrayList<>();
        Set<Throwable> seen = Sets.newIdentityHashSet();
        for (Throwable current = failure; current != null && seen.add(current); current = current.getCause()) {
            classes.add(current.getClass().getName());
        }
        String description = String.join(", ", classes);
        if (failure instanceof TrinoException trinoException) {
            return trinoException.getErrorCode().getName() + trinoException.getLocation().map(location -> " at " + location).orElse("") + " (" + description + ")";
        }
        return description;
    }
}
