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
package io.trino.plugin.hive.util;

import java.util.HashSet;
import java.util.Set;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileSplit;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Map;

import static org.testng.Assert.assertEquals;

public class TestCustomSplitManager
{
    @Test
    public void testCustomSplitConverterRoundTrip() throws IOException
    {
        Path expectedPath = new Path("s3://test/path");
        long expectedStart = 1L;
        long expectedLength = 2L;
        String[] expectedLocations = new String[] {"one", "two"};
        int customField = 100;

        FileSplit baseSplit = new FileSplit(expectedPath, expectedStart, expectedLength, expectedLocations);
        FileSplit customSplit = new CustomSplit(baseSplit, customField);

        Set<CustomSplitConverter> converters = new HashSet<>();
        converters.add(new TestSplitConverter());
        CustomSplitManager manager = new CustomSplitManager(converters);

        // Test conversion of CustomSplit -> customSplitInfo
        Map<String, String> customSplitInfo = manager.extractCustomSplitInfo(customSplit);

        // Test conversion of (customSplitInfo + baseSplit) -> CustomSplit
        FileSplit recreatedSplit = manager.recreateSplitWithCustomInfo(baseSplit, customSplitInfo);

        assertEquals(recreatedSplit.getPath(), expectedPath);
        assertEquals(recreatedSplit.getStart(), expectedStart);
        assertEquals(recreatedSplit.getLength(), expectedLength);
        assertEquals(recreatedSplit.getLocations(), expectedLocations);
        assertEquals(((CustomSplit) recreatedSplit).getCustomField(), customField);
        assertEquals(((CustomSplit) recreatedSplit).getEmbeddedSplit(), baseSplit);
    }
}
