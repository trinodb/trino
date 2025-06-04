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

import java.util.Iterator;

import static java.lang.String.format;

public class DefaultPrinter
        implements PatternVisitor
{
    private final StringBuilder result = new StringBuilder();
    private int level;

    public String result()
    {
        return result.toString();
    }

    @Override
    public void visitTypeOf(TypeOfPattern<?> pattern)
    {
        visitPrevious(pattern);
        appendLine("typeOf(%s)", pattern.expectedClass().getSimpleName());
    }

    @Override
    public void visitWith(WithPattern<?> pattern)
    {
        visitPrevious(pattern);
        appendLine("with(%s)", pattern.getProperty().getName());
        level += 1;
        pattern.getPattern().accept(this);
        level -= 1;
    }

    @Override
    public void visitCapture(CapturePattern<?> pattern)
    {
        visitPrevious(pattern);
        appendLine("capturedAs(%s)", pattern.capture().description());
    }

    @Override
    public void visitEquals(EqualsPattern<?> pattern)
    {
        visitPrevious(pattern);
        appendLine("equals(%s)", pattern.expectedValue());
    }

    @Override
    public void visitFilter(FilterPattern<?> pattern)
    {
        visitPrevious(pattern);
        appendLine("filter(%s)", pattern.predicate());
    }

    @Override
    public void visitOr(OrPattern<?> pattern)
    {
        visitPrevious(pattern);
        level += 1;
        Iterator<?> iterator = pattern.getPatterns().iterator();
        while (iterator.hasNext()) {
            Pattern<?> subPattern = (Pattern<?>) iterator.next();
            subPattern.accept(this);
            if (iterator.hasNext()) {
                appendLine("or");
            }
        }
        level -= 1;
    }

    private void appendLine(String template, Object... arguments)
    {
        result.append("\t".repeat(level)).append(format(template + "\n", arguments));
    }
}
