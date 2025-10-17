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

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

record CharacterRange(CharacterRangeBound start, CharacterRangeBound end)
        implements CharacterGroupElement
{
    public CharacterRange
    {
        requireNonNull(start, "start is null");
        requireNonNull(end, "end is null");
        checkArgument(start.effectiveCodePoint() <= end.effectiveCodePoint(), "start is greater than end");
    }

    @Override
    public <R> R accept(RegexTreeVisitor<R> visitor)
    {
        return visitor.visitCharacterRange(this);
    }
}
