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
package io.trino.spi.type;

/// A datetime field that can appear in an interval qualifier.
///
/// The declaration order, exposed as [#code()], runs from most significant
/// (`YEAR`) to least significant (`SECOND`). It is the stable encoding used to
/// carry an interval qualifier in the numeric parameters of an interval type's
/// [TypeDescriptor], so the order must not change.
public enum IntervalField
{
    YEAR("year"),
    MONTH("month"),
    DAY("day"),
    HOUR("hour"),
    MINUTE("minute"),
    SECOND("second");

    private static final IntervalField[] BY_CODE = values();

    private final String keyword;

    IntervalField(String keyword)
    {
        this.keyword = keyword;
    }

    /// The SQL keyword naming this field, in lower case (for example `hour`).
    public String keyword()
    {
        return keyword;
    }

    /// The stable significance code: `0` for `YEAR` through `5` for `SECOND`.
    /// A smaller code denotes a more significant field.
    public int code()
    {
        return ordinal();
    }

    public static IntervalField fromCode(int code)
    {
        if (code < 0 || code >= BY_CODE.length) {
            throw new IllegalArgumentException("Invalid interval field code: " + code);
        }
        return BY_CODE[code];
    }
}
