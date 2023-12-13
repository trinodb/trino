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

import io.trino.matching.example.rel.FilterNode;
import io.trino.matching.example.rel.JoinNode;
import io.trino.matching.example.rel.ProjectNode;
import io.trino.matching.example.rel.RelNode;
import io.trino.matching.example.rel.ScanNode;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.google.common.collect.MoreCollectors.onlyElement;
import static com.google.common.collect.MoreCollectors.toOptional;
import static io.trino.matching.Capture.newCapture;
import static io.trino.matching.Pattern.any;
import static io.trino.matching.Pattern.typeOf;
import static io.trino.matching.example.rel.Patterns.filter;
import static io.trino.matching.example.rel.Patterns.plan;
import static io.trino.matching.example.rel.Patterns.project;
import static io.trino.matching.example.rel.Patterns.scan;
import static io.trino.matching.example.rel.Patterns.source;
import static io.trino.matching.example.rel.Patterns.tableName;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestMatcher
{
    @Test
    public void trivialMatchers()
    {
        //any
        assertMatch(any(), 42);
        assertMatch(any(), "John Doe");

        //class based
        assertMatch(typeOf(Integer.class), 42);
        assertMatch(typeOf(Number.class), 42);
        assertNoMatch(typeOf(Integer.class), "John Doe");

        //predicate-based
        assertMatch(typeOf(Integer.class).matching(x -> x > 0), 42);
        assertNoMatch(typeOf(Integer.class).matching(x -> x > 0), -1);
    }

    @Test
    public void matchObject()
    {
        assertMatch(project(), new ProjectNode(null));
        assertNoMatch(project(), new ScanNode("t"));
    }

    @Test
    public void propertyMatchers()
    {
        Pattern<String> aString = typeOf(String.class);
        Property<String, ?, Integer> length = Property.property("length", String::length);
        String string = "a";

        assertMatch(aString.with(length.equalTo(1)), string);
        assertMatch(project().with(source().matching(scan())), new ProjectNode(new ScanNode("T")));
        assertMatch(aString.with(length.matching(any())), string);
        assertMatch(aString.with(length.matching(x -> x > 0)), string);
        assertMatch(aString.with(length.matching((Number x) -> x.intValue() > 0)), string);

        assertNoMatch(aString.with(length.equalTo(0)), string);
        assertNoMatch(project().with(source().matching(scan())), new ProjectNode(new ProjectNode(new ScanNode("T"))));
        assertNoMatch(aString.with(length.matching(typeOf(Void.class))), string);
        assertNoMatch(aString.with(length.matching(x -> x < 1)), string);
        assertNoMatch(aString.with(length.matching((Number x) -> x.intValue() < 1)), string);
    }

    @Test
    public void matchNestedProperties()
    {
        Pattern<ProjectNode> pattern = project().with(source().matching(scan()));

        assertMatch(pattern, new ProjectNode(new ScanNode("t")));
        assertNoMatch(pattern, new ScanNode("t"));
        //TODO this needs a custom Option type to work , or NPEs will happen.
        //Optional does not allow null values.
        //assertNoMatch(pattern, new ProjectNode(null));
        assertNoMatch(pattern, new ProjectNode(new ProjectNode(null)));
    }

    @Test
    public void matchAdditionalProperties()
    {
        String matchedValue = "A little string.";

        Pattern<String> pattern = typeOf(String.class)
                .matching(s -> s.startsWith("A"))
                .matching((CharSequence s) -> s.length() > 7);

        assertMatch(pattern, matchedValue);
    }

    @Test
    public void optionalProperties()
    {
        Property<RelNode, ?, RelNode> onlySource = Property.optionalProperty("onlySource", node ->
                Optional.of(node.getSources())
                        .filter(sources -> sources.size() == 1)
                        .map((List<RelNode> sources) -> sources.get(0)));

        Pattern<RelNode> relNodeWithExactlyOneSource = plan()
                .with(onlySource.matching(any()));

        assertMatch(relNodeWithExactlyOneSource, new ProjectNode(new ScanNode("t")));
        assertNoMatch(relNodeWithExactlyOneSource, new ScanNode("t"));
        assertNoMatch(relNodeWithExactlyOneSource, new JoinNode(new ScanNode("t"), new ScanNode("t")));
    }

    @Test
    public void capturingMatchesInATypesafeManner()
    {
        Capture<FilterNode> filter = newCapture();
        Capture<ScanNode> scan = newCapture();
        Capture<String> name = newCapture();

        Pattern<ProjectNode> pattern = project()
                .with(source().matching(filter().capturedAs(filter)
                        .with(source().matching(scan().capturedAs(scan)
                                .with(tableName().capturedAs(name))))));

        ProjectNode tree = new ProjectNode(new FilterNode(new ScanNode("orders"), null));

        Match match = assertMatch(pattern, tree);
        //notice the concrete type despite no casts:
        FilterNode capturedFilter = match.capture(filter);
        assertThat(tree.getSource()).isEqualTo(capturedFilter);
        assertThat(((FilterNode) tree.getSource()).getSource()).isEqualTo(match.capture(scan));
        assertThat("orders").isEqualTo(match.capture(name));
    }

    @Test
    public void noMatch()
    {
        Capture<Void> impossible = newCapture();
        Pattern<Void> pattern = typeOf(Void.class).capturedAs(impossible);

        Optional<Match> match = pattern.match(42)
                .collect(toOptional());

        assertThat(match.isPresent()).isFalse();
    }

    @Test
    public void unknownCaptureIsAnError()
    {
        Pattern<?> pattern = any();
        Capture<?> unknownCapture = newCapture();

        Match match = pattern.match(42)
                .collect(onlyElement());

        assertThatThrownBy(() -> match.capture(unknownCapture))
                .isInstanceOf(NoSuchElementException.class)
                .hasMessageContaining("unknown Capture");
    }

    @Test
    public void nullNotMatchedByDefault()
    {
        assertNoMatch(any(), null);
        assertNoMatch(typeOf(Integer.class), null);
    }

    @Test
    public void contextIsPassedToPropertyFunction()
    {
        Pattern<?> pattern = any().with(
                Property.property(
                        "non null",
                        (Object value, AtomicBoolean context) -> {
                            context.set(true);
                            return value != null;
                        }).equalTo(true));

        AtomicBoolean wasContextUsed = new AtomicBoolean();
        assertThat(pattern.match("object", wasContextUsed)
                .collect(onlyElement())).isNotNull();
        assertThat(wasContextUsed.get()).isEqualTo(true);
    }

    @Test
    public void contextIsPassedToPredicate()
    {
        Pattern<?> pattern = any().matching(
                (Object value, AtomicBoolean context) -> {
                    context.set(true);
                    return value != null;
                });

        AtomicBoolean wasContextUsed = new AtomicBoolean();
        assertThat(pattern.match("object", wasContextUsed)
                .collect(onlyElement())).isNotNull();
        assertThat(wasContextUsed.get()).isEqualTo(true);
    }

    private <T> Match assertMatch(Pattern<T> pattern, T object)
    {
        return pattern.match(object)
                .collect(onlyElement());
    }

    private <T> void assertNoMatch(Pattern<T> pattern, Object expectedNoMatch)
    {
        Optional<Match> match = pattern.match(expectedNoMatch)
                .collect(toOptional());
        assertThat(match.isPresent()).isFalse();
    }
}
