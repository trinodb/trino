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
package io.prestosql.spi.connector;

import io.prestosql.spi.WarningCode;
import io.prestosql.spi.WarningCodeSupplier;

public enum StandardWarningCode
        implements WarningCodeSupplier
{
    TOO_MANY_STAGES(0x0000_0001),
    REDUNDANT_ORDER_BY(0x0000_0002),
    USER_MEMORY_LIMIT_OVER_THRESHOLD_VAL(0x0000_0003),
    TOTAL_MEMORY_LIMIT_OVER_THRESHOLD_VAL(0x0000_0004),
    TOTAL_CPU_TIME_OVER_THRESHOLD_VAL(0x0000_0005),
    STAGE_SKEW(0x0000_0006),
    BETTER_JOIN_ORDERING(0x0000_0007),
    TABLE_DEPRECATED_WARNINGS(0x0000_0008),
    UDF_DEPRECATED_WARNINGS(0x0000_009),
    SESSION_DEPRECATED_WARNINGS(0x0000_000A),

    /**/;
    private final WarningCode warningCode;

    StandardWarningCode(int code)
    {
        warningCode = new WarningCode(code, name());
    }

    @Override
    public WarningCode toWarningCode()
    {
        return warningCode;
    }
}
