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

import java.util.Iterator;

import static java.lang.String.format;

public class PatternPrinter
{
    private final StringBuilder result = new StringBuilder();
    private int level;

    public String result()
    {
        return result.toString();
    }

    public <T> void print(Pattern<?> pattern)
    {
        pattern.previous().ifPresent(this::print);
        switch (pattern) {
            case TypeOfPattern<?> typeOfPattern -> appendLine("typeOf(%s)", typeOfPattern.expectedClass().getSimpleName());
            case WithPattern<?> withPattern -> {
                appendLine("with(%s)", withPattern.getProperty().name());
                level += 1;
                print(withPattern.getPattern());
                level -= 1;
            }
            case CapturePattern<?> capturePattern -> appendLine("capturedAs($%d)", capturePattern.capture().number());
            case EqualsPattern<?> equalsPattern -> appendLine("equals(%s)", equalsPattern.expectedValue());
            case FilterPattern<?> filterPattern -> appendLine("filter(%s)", filterPattern.predicate());
            case OrPattern<?> orPattern -> {
                level += 1;
                Iterator<?> iterator = orPattern.getPatterns().iterator();
                while (iterator.hasNext()) {
                    Pattern<?> subPattern = (Pattern<?>) iterator.next();
                    print(subPattern);
                    if (iterator.hasNext()) {
                        appendLine("or");
                    }
                }
                level -= 1;
            }
            case CustomPattern<?> customPattern -> appendLine(customPattern.print());
        }
    }

    private void appendLine(String template, Object... arguments)
    {
        result.append("\t".repeat(level)).append(format(template + "\n", arguments));
    }
}
