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
package io.trino.plugin.bigquery;

import com.google.common.collect.ImmutableMap;
import io.airlift.units.Duration;
import org.testng.annotations.Test;

import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.MINUTES;

public class TestBigQueryConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(BigQueryConfig.class)
                .setProjectId(null)
                .setParentProjectId(null)
                .setParallelism(null)
                .setViewMaterializationProject(null)
                .setViewMaterializationDataset(null)
                .setMaxReadRowsRetries(3)
                .setCaseInsensitiveNameMatching(false)
                .setViewsCacheTtl(new Duration(15, MINUTES))
                .setServiceCacheTtl(new Duration(3, MINUTES))
                .setViewsEnabled(false));
    }

    @Test
    public void testExplicitPropertyMappingsWithCredentialsKey()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("bigquery.project-id", "pid")
                .put("bigquery.parent-project-id", "ppid")
                .put("bigquery.parallelism", "20")
                .put("bigquery.views-enabled", "true")
                .put("bigquery.view-materialization-project", "vmproject")
                .put("bigquery.view-materialization-dataset", "vmdataset")
                .put("bigquery.max-read-rows-retries", "10")
                .put("bigquery.case-insensitive-name-matching", "true")
                .put("bigquery.views-cache-ttl", "1m")
                .put("bigquery.service-cache-ttl", "10d")
                .buildOrThrow();

        BigQueryConfig expected = new BigQueryConfig()
                .setProjectId("pid")
                .setParentProjectId("ppid")
                .setParallelism(20)
                .setViewsEnabled(true)
                .setViewMaterializationProject("vmproject")
                .setViewMaterializationDataset("vmdataset")
                .setMaxReadRowsRetries(10)
                .setCaseInsensitiveNameMatching(true)
                .setViewsCacheTtl(new Duration(1, MINUTES))
                .setServiceCacheTtl(new Duration(10, DAYS));

        assertFullMapping(properties, expected);
    }
}
