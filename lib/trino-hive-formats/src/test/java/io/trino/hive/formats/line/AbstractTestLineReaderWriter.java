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
package io.trino.hive.formats.line;

import com.google.common.collect.ContiguousSet;
import com.google.common.collect.DiscreteDomain;
import com.google.common.collect.Range;
import io.trino.hadoop.HadoopNative;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Set;

import static io.airlift.slice.SizeOf.SIZE_OF_LONG;
import static java.util.Collections.nCopies;
import static java.util.stream.Collectors.toList;

public abstract class AbstractTestLineReaderWriter
{
    static {
        HadoopNative.requireHadoopNative();
    }

    @Test
    public void testBinarySequence()
            throws Exception
    {
        testRoundTrip(
                intsBetween(0, 31_234).stream()
                        .filter(i -> i % 19 == 0)
                        .map(Object::toString)
                        .collect(toList()));
    }

    @Test
    public void testEmptyBinarySequence()
            throws Exception
    {
        testRoundTrip(nCopies(3_000, ""));
    }

    private static Set<Integer> intsBetween(int lowerExclusive, int upperInclusive)
    {
        return ContiguousSet.create(Range.openClosed(lowerExclusive, upperInclusive), DiscreteDomain.integers());
    }

    protected abstract void testRoundTrip(List<String> values)
            throws Exception;

    protected static LineBuffer createLineBuffer(List<String> lines)
    {
        int maxLineLength = lines.stream()
                .mapToInt(String::length)
                .sum();
        maxLineLength += SIZE_OF_LONG;
        LineBuffer lineBuffer = new LineBuffer(1, maxLineLength);
        return lineBuffer;
    }
}
