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
package io.trino.json.regex;

import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;

import static com.google.common.base.Preconditions.checkArgument;

record SingleCharacterEscape(int codePoint)
        implements CharacterClassEscape, CharacterRangeBound
{
    private static final Int2IntMap EFFECTIVE_CODE_POINTS = new Int2IntOpenHashMap();

    static {
        EFFECTIVE_CODE_POINTS.put('n', '\n');
        EFFECTIVE_CODE_POINTS.put('r', '\r');
        EFFECTIVE_CODE_POINTS.put('t', '\t');
        EFFECTIVE_CODE_POINTS.put('\\', '\\');
        EFFECTIVE_CODE_POINTS.put('|', '|');
        EFFECTIVE_CODE_POINTS.put('.', '.');
        EFFECTIVE_CODE_POINTS.put('-', '-');
        EFFECTIVE_CODE_POINTS.put('^', '^');
        EFFECTIVE_CODE_POINTS.put('?', '?');
        EFFECTIVE_CODE_POINTS.put('*', '*');
        EFFECTIVE_CODE_POINTS.put('+', '+');
        EFFECTIVE_CODE_POINTS.put('{', '{');
        EFFECTIVE_CODE_POINTS.put('}', '}');
        EFFECTIVE_CODE_POINTS.put('(', '(');
        EFFECTIVE_CODE_POINTS.put(')', ')');
        EFFECTIVE_CODE_POINTS.put('[', '[');
        EFFECTIVE_CODE_POINTS.put(']', ']');
        EFFECTIVE_CODE_POINTS.put('$', '$');
    }

    SingleCharacterEscape
    {
        checkArgument(EFFECTIVE_CODE_POINTS.containsKey(codePoint), "Unrecognized escape code point: %s", codePoint);
    }

    @Override
    public <R> R accept(RegexTreeVisitor<R> visitor)
    {
        return visitor.visitSingleCharacterEscape(this);
    }

    @Override
    public int effectiveCodePoint()
    {
        return EFFECTIVE_CODE_POINTS.get(codePoint);
    }
}
