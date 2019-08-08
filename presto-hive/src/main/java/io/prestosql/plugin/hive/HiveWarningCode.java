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
package io.prestosql.plugin.hive;

import io.prestosql.spi.WarningCode;
import io.prestosql.spi.WarningCodeSupplier;

public enum HiveWarningCode
        implements WarningCodeSupplier
{
    PREFERRED_FILE_FORMAT_SUGGESTION(0),
    TOO_MANY_PARTITIONS(1);

    private final WarningCode warningCode;

    HiveWarningCode(int code)
    {
        warningCode = new WarningCode(code + 0x0100_0000, name());
    }

    @Override
    public WarningCode toWarningCode()
    {
        return warningCode;
    }
}
