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
package io.trino.plugin.hive;

import com.google.common.collect.ImmutableMap;
import io.airlift.units.Duration;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;

public class TestRecordingMetastoreConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(RecordingMetastoreConfig.class)
                .setRecordingPath(null)
                .setRecordingDuration(new Duration(10, TimeUnit.MINUTES))
                .setReplay(false));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("hive.metastore-recording-path", "/foo/bar")
                .put("hive.metastore-recording-duration", "42s")
                .put("hive.replay-metastore-recording", "true")
                .buildOrThrow();

        RecordingMetastoreConfig expected = new RecordingMetastoreConfig()
                .setRecordingPath("/foo/bar")
                .setRecordingDuration(new Duration(42, TimeUnit.SECONDS))
                .setReplay(true);

        assertFullMapping(properties, expected);
    }
}
