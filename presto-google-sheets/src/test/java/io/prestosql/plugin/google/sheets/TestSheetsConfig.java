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
package io.prestosql.plugin.google.sheets;

import com.google.common.collect.ImmutableMap;
import io.airlift.units.Duration;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;

public class TestSheetsConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(SheetsConfig.class)
                .setCredentialsFilePath(null)
                .setMetadataSheetId(null)
                .setSheetsDataMaxCacheSize(1000)
                .setSheetsDataExpireAfterWrite(new Duration(5, TimeUnit.MINUTES)));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        SheetsConfig expected = new SheetsConfig()
                .setCredentialsFilePath("/foo/bar/credentials.json")
                .setMetadataSheetId("foo_bar_sheet_id#Sheet1")
                .setSheetsDataMaxCacheSize(2000)
                .setSheetsDataExpireAfterWrite(new Duration(10, TimeUnit.MINUTES));
        assertFullMapping(getProperties("/foo/bar/credentials.json", "foo_bar_sheet_id#Sheet1", 2000, "10m"), expected);
    }

    static Map<String, String> getProperties(String credentialsPath, String metadataSheetId, int cacheSize, String cacheDuration)
    {
        return new ImmutableMap.Builder<String, String>()
                .put("credentials-path", credentialsPath)
                .put("metadata-sheet-id", metadataSheetId)
                .put("sheets-data-max-cache-size", String.valueOf(cacheSize))
                .put("sheets-data-expire-after-write", cacheDuration)
                .build();
    }
}
