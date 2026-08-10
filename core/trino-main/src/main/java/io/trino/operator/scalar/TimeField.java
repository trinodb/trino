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
import io.trino.spi.TrinoException;

import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static java.nio.charset.StandardCharsets.US_ASCII;
import static java.util.Locale.ENGLISH;

/**
 * The field of a TIME or TIME WITH TIME ZONE value that {@code date_trunc}, {@code date_add} and
 * {@code date_diff} operate on.
 */
public enum TimeField
{
    MILLISECOND("millisecond"),
    SECOND("second"),
    MINUTE("minute"),
    HOUR("hour");

    private static final TimeField[] VALUES = values();

    private final byte[] name;

    TimeField(String name)
    {
        this.name = name.getBytes(US_ASCII);
    }

    public static TimeField match(Slice unit)
    {
        for (TimeField field : VALUES) {
            if (field.matches(unit)) {
                return field;
            }
        }
        throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "'" + unit.toStringUtf8().toLowerCase(ENGLISH) + "' is not a valid TIME field");
    }

    private boolean matches(Slice unit)
    {
        if (unit.length() != name.length) {
            return false;
        }
        for (int i = 0; i < name.length; i++) {
            // Each lowercase letter has an ASCII code 0x20 more than the corresponding uppercase letter
            if ((unit.getByteUnchecked(i) | 0x20) != name[i]) {
                return false;
            }
        }
        return true;
    }
}
