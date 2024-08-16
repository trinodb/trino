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
package io.trino.matching;

import io.trino.matching.pattern.CapturePattern;
import io.trino.matching.pattern.EqualsPattern;
import io.trino.matching.pattern.FilterPattern;
import io.trino.matching.pattern.OrPattern;
import io.trino.matching.pattern.TypeOfPattern;
import io.trino.matching.pattern.WithPattern;

import java.util.Optional;

public interface PatternVisitor
{
    void visitTypeOf(TypeOfPattern<?> pattern);

    void visitWith(WithPattern<?> pattern);

    void visitCapture(CapturePattern<?> pattern);

    void visitEquals(EqualsPattern<?> equalsPattern);

    void visitFilter(FilterPattern<?> pattern);

    void visitOr(OrPattern<?> pattern);

    default void visitPrevious(Pattern<?> pattern)
    {
        Optional<Pattern<?>> previous = pattern.previous();
        if (previous.isPresent()) {
            previous.get().accept(this);
        }
    }
}
