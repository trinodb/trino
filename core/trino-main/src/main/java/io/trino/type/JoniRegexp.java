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
package io.trino.type;

import io.airlift.joni.Matcher;
import io.airlift.joni.Regex;
import io.airlift.slice.Slice;

import static java.util.Objects.requireNonNull;

public final class JoniRegexp
{
    private final Slice pattern;
    private final Regex regex;

    public JoniRegexp(Slice pattern, Regex regex)
    {
        this.pattern = requireNonNull(pattern, "pattern is null");
        this.regex = requireNonNull(regex, "regex is null");
    }

    public Slice pattern()
    {
        return pattern;
    }

    public Regex regex()
    {
        return regex;
    }

    public Matcher matcher(byte[] bytes)
    {
        return regex.matcher(bytes);
    }

    // regex is always compiled from pattern with the same options (see JoniRegexpCasts), so the pattern identifies it
    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        JoniRegexp other = (JoniRegexp) obj;
        return pattern.equals(other.pattern);
    }

    @Override
    public int hashCode()
    {
        return pattern.hashCode();
    }

    @Override
    public String toString()
    {
        return pattern.toStringUtf8();
    }
}
