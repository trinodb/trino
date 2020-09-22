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

public enum HiveTimestampPrecision
{
    MILLISECONDS(3), MICROSECONDS(6), NANOSECONDS(9);

    private final int precision;

    HiveTimestampPrecision(int precision)
    {
        this.precision = precision;
    }

    public int getPrecision()
    {
        return precision;
    }
}
