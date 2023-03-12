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
package io.trino.likematcher;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;

sealed interface Pattern
        permits Pattern.Any, Pattern.Literal
{
    record Literal(String value)
            implements Pattern
    {
        @Override
        public String toString()
        {
            return value;
        }
    }

    record Any(int min, boolean unbounded)
            implements Pattern
    {
        public Any
        {
            checkArgument(min > 0 || unbounded, "Any must be unbounded or require at least 1 character");
        }

        @Override
        public String toString()
        {
            return format("{%s%s}", min, unbounded ? "+" : "");
        }
    }
}
