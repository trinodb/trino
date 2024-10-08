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
package io.trino.matching.pattern;

import io.trino.matching.Captures;
import io.trino.matching.Match;
import io.trino.matching.Pattern;
import io.trino.matching.PatternVisitor;

import java.util.List;
import java.util.stream.Stream;

public class OrPattern<T>
        extends Pattern<T>
{
    private final List<Pattern<T>> patterns;

    public OrPattern(List<Pattern<T>> patterns, Pattern<T> previous)
    {
        super(previous);
        this.patterns = patterns;
    }

    public List<Pattern<T>> getPatterns()
    {
        return patterns;
    }

    @Override
    public <C> Stream<Match> accept(Object object, Captures captures, C context)
    {
        return patterns.stream()
                .flatMap(pattern -> pattern.accept(object, captures, context));
    }

    @Override
    public void accept(PatternVisitor patternVisitor)
    {
        patternVisitor.visitOr(this);
    }
}
