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

class MatchLabel
        implements Instruction
{
    private final int label;

    public MatchLabel(int label)
    {
        this.label = label;
    }

    public int getLabel()
    {
        return label;
    }

    @Override
    public String toString()
    {
        return "match " + label;
    }

    @Override
    public Type type()
    {
        return Type.MATCH_LABEL;
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
        return label == ((MatchLabel) obj).label;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(label);
    }
}
