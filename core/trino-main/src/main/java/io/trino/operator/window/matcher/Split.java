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
package io.trino.operator.window.matcher;

import java.util.Objects;

import static java.lang.String.format;

class Split
        implements Instruction
{
    private final int first;
    private final int second;

    public Split(int first, int second)
    {
        this.first = first;
        this.second = second;
    }

    public int getFirst()
    {
        return first;
    }

    public int getSecond()
    {
        return second;
    }

    @Override
    public String toString()
    {
        return format("split %s, %s", first, second);
    }

    @Override
    public Type type()
    {
        return Type.SPLIT;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        Split o = (Split) obj;
        return first == o.first &&
                second == o.second;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(first, second);
    }
}
