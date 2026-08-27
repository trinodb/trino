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
package io.trino.operator.scalar;

import io.airlift.slice.Slice;
import io.trino.FullConnectorSession;
import io.trino.Session;
import io.trino.SystemSessionProperties;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.function.LiteralParameter;
import io.trino.spi.function.LiteralParameters;
import io.trino.spi.function.OperatorType;
import io.trino.spi.function.ScalarOperator;
import io.trino.spi.function.SqlType;
import io.trino.type.CharVarcharCoercion;

import static io.trino.spi.type.Chars.padSpaces;
import static io.trino.spi.type.Varchars.truncateToLength;

/// `CHAR` to `VARCHAR` cast whose behavior follows the session's char/varchar coercion
/// direction ([SystemSessionProperties#getCharVarcharCoercion(Session)].
public final class CharToVarcharCast
{
    private CharToVarcharCast() {}

    @ScalarOperator(value = OperatorType.CAST, neverFails = true)
    @SqlType("varchar(y)")
    @LiteralParameters({"x", "y"})
    public static Slice charToVarcharCast(ConnectorSession session, @LiteralParameter("x") long x, @LiteralParameter("y") long y, @SqlType("char(x)") Slice slice)
    {
        if (((FullConnectorSession) session).getCharVarcharCoercion() == CharVarcharCoercion.LEGACY) {
            // Legacy: re-pad to the declared CHAR length, truncating to the target VARCHAR length when it is shorter.
            if (x <= y) {
                return padSpaces(slice, (int) x);
            }
            return padSpaces(truncateToLength(slice, (int) y), (int) y);
        }
        // CHAR values are stored without trailing spaces; yield the unpadded value, truncated to the target length.
        return truncateToLength(slice, (int) y);
    }
}
