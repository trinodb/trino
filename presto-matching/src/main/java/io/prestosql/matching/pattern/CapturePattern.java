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
package io.prestosql.matching.pattern;

import io.prestosql.matching.Capture;
import io.prestosql.matching.Captures;
import io.prestosql.matching.Match;
import io.prestosql.matching.Pattern;
import io.prestosql.matching.PatternVisitor;

import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;

public class CapturePattern<T>
        extends Pattern<T>
{
    private final Capture<T> capture;

    public CapturePattern(Capture<T> capture, Pattern<T> previous)
    {
        super(previous);
        this.capture = requireNonNull(capture, "capture is null");
    }

    public Capture<T> capture()
    {
        return capture;
    }

    @Override
    public <C> Stream<Match> accept(Object object, Captures captures, C context)
    {
        Captures newCaptures = captures.addAll(Captures.ofNullable(capture, (T) object));
        return Stream.of(Match.of(newCaptures));
    }

    @Override
    public void accept(PatternVisitor patternVisitor)
    {
        patternVisitor.visitCapture(this);
    }
}
