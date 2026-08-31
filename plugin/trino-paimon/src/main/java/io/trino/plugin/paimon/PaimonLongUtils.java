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
package io.trino.plugin.paimon;

import static com.google.common.base.Preconditions.checkArgument;

final class PaimonLongUtils
{
    private PaimonLongUtils() {}

    static long saturatedAdd(long currentValue, long increment, String incrementDescription)
    {
        checkArgument(currentValue >= 0, "current value must be non-negative: %s", currentValue);
        checkArgument(increment >= 0, "%s must be non-negative: %s", incrementDescription, increment);
        if (Long.MAX_VALUE - currentValue < increment) {
            return Long.MAX_VALUE;
        }
        return currentValue + increment;
    }
}
