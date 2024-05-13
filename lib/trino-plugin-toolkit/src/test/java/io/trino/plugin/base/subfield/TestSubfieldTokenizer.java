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
package io.trino.plugin.base.subfield;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Streams;
import io.trino.spi.TrinoException;
import org.junit.jupiter.api.Test;

import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatException;

// Class to test SubfieldTokenizer. Direct referenced from Presto
public class TestSubfieldTokenizer
{
    @Test
    public void test()
    {
        List<Subfield.PathElement> elements = ImmutableList.of(
                new Subfield.NestedField("b"),
                new Subfield.LongSubscript(2),
                new Subfield.LongSubscript(-1),
                new Subfield.StringSubscript("z"),
                Subfield.allSubscripts(),
                new Subfield.StringSubscript("34"),
                new Subfield.StringSubscript("b \"test\""),
                new Subfield.StringSubscript("\"abc"),
                new Subfield.StringSubscript("abc\""),
                new Subfield.StringSubscript("ab\"cde"),
                new Subfield.StringSubscript("a.b[\"hello\uDBFF\"]"));

        for (Subfield.PathElement element : elements) {
            assertPath(new Subfield("a", ImmutableList.of(element)));
        }

        for (Subfield.PathElement element : elements) {
            for (Subfield.PathElement secondElement : elements) {
                assertPath(new Subfield("a", ImmutableList.of(element, secondElement)));
            }
        }

        for (Subfield.PathElement element : elements) {
            for (Subfield.PathElement secondElement : elements) {
                for (Subfield.PathElement thirdElement : elements) {
                    assertPath(new Subfield("a", ImmutableList.of(element, secondElement, thirdElement)));
                }
            }
        }
    }

    private static void assertPath(Subfield path)
    {
        SubfieldTokenizer tokenizer = new SubfieldTokenizer(path.serialize());
        assertThat(tokenizer.hasNext());
        assertThat(new Subfield(((Subfield.NestedField) tokenizer.next()).getName(), Streams.stream(tokenizer).collect(toImmutableList())).equals(path));
    }

    @Test
    public void testColumnNames()
    {
        assertPath(new Subfield("#bucket", ImmutableList.of()));
        assertPath(new Subfield("$bucket", ImmutableList.of()));
        assertPath(new Subfield("apollo-11", ImmutableList.of()));
        assertPath(new Subfield("a/b/c:12", ImmutableList.of()));
        assertPath(new Subfield("@basis", ImmutableList.of()));
        assertPath(new Subfield("@basis|city_id", ImmutableList.of()));
        assertPath(new Subfield("a and b", ImmutableList.of()));
    }

    @Test
    public void testInvalidPaths()
    {
        assertInvalidPath("a[b]");
        assertInvalidPath("a[2");
        assertInvalidPath("a.*");
        assertInvalidPath("a[2].[3].");
    }

    private void assertInvalidPath(String path)
    {
        SubfieldTokenizer tokenizer = new SubfieldTokenizer(path);

        try {
            Streams.stream(tokenizer).collect(toImmutableList());
            assertThatException();
        }
        catch (TrinoException trinoException) {
            assertThat(trinoException.getErrorCode() == INVALID_FUNCTION_ARGUMENT.toErrorCode());
        }
    }
}
