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
import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestBigQueryConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(BigQueryConfig.class)
                .setProjectId(null)
                .setParentProjectId(null)
                .setParallelism(null)
                .setViewExpireDuration(new Duration(24, HOURS))
                .setSkipViewMaterialization(false)
                .setViewMaterializationProject(null)
                .setViewMaterializationDataset(null)
                .setMaxReadRowsRetries(3)
                .setCaseInsensitiveNameMatching(false)
                .setViewsCacheTtl(new Duration(15, MINUTES))
                .setServiceCacheTtl(new Duration(3, MINUTES))
                .setMetadataCacheTtl(new Duration(0, MILLISECONDS))
                .setViewsEnabled(false)
                .setArrowSerializationEnabled(false)
                .setQueryResultsCacheEnabled(false)
                .setRpcInitialChannelCount(1)
                .setMinRpcPerChannel(0)
                .setMaxRpcPerChannel(Integer.MAX_VALUE)
                .setRpcMinChannelCount(1)
                .setRpcMaxChannelCount(1));
    }

    @Test
    public void testExplicitPropertyMappingsWithCredentialsKey()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("bigquery.project-id", "pid")
                .put("bigquery.parent-project-id", "ppid")
                .put("bigquery.parallelism", "20")
                .put("bigquery.views-enabled", "true")
                .put("bigquery.experimental.arrow-serialization.enabled", "true")
                .put("bigquery.view-expire-duration", "30m")
                .put("bigquery.skip-view-materialization", "true")
                .put("bigquery.view-materialization-project", "vmproject")
                .put("bigquery.view-materialization-dataset", "vmdataset")
                .put("bigquery.max-read-rows-retries", "10")
                .put("bigquery.case-insensitive-name-matching", "true")
                .put("bigquery.views-cache-ttl", "1m")
                .put("bigquery.service-cache-ttl", "10d")
                .put("bigquery.metadata.cache-ttl", "5d")
                .put("bigquery.query-results-cache.enabled", "true")
                .put("bigquery.channel-pool.initial-size", "11")
                .put("bigquery.channel-pool.min-size", "12")
                .put("bigquery.channel-pool.max-size", "13")
                .put("bigquery.channel-pool.min-rpc-per-channel", "14")
                .put("bigquery.channel-pool.max-rpc-per-channel", "15")
                .buildOrThrow();

        BigQueryConfig expected = new BigQueryConfig()
                .setProjectId("pid")
                .setParentProjectId("ppid")
                .setParallelism(20)
                .setViewsEnabled(true)
                .setArrowSerializationEnabled(true)
                .setViewExpireDuration(new Duration(30, MINUTES))
                .setSkipViewMaterialization(true)
                .setViewMaterializationProject("vmproject")
                .setViewMaterializationDataset("vmdataset")
                .setMaxReadRowsRetries(10)
                .setCaseInsensitiveNameMatching(true)
                .setViewsCacheTtl(new Duration(1, MINUTES))
                .setServiceCacheTtl(new Duration(10, DAYS))
                .setMetadataCacheTtl(new Duration(5, DAYS))
                .setQueryResultsCacheEnabled(true)
                .setRpcInitialChannelCount(11)
                .setRpcMinChannelCount(12)
                .setRpcMaxChannelCount(13)
                .setMinRpcPerChannel(14)
                .setMaxRpcPerChannel(15);

        assertFullMapping(properties, expected);
    }

    @Test
    public void testInvalidViewSetting()
    {
        assertThatThrownBy(() -> new BigQueryConfig()
                .setViewExpireDuration(new Duration(5, MINUTES))
                .setViewsCacheTtl(new Duration(10, MINUTES))
                .validate())
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("View expiration duration must be longer than view cache TTL");

        assertThatThrownBy(() -> new BigQueryConfig()
                .setSkipViewMaterialization(true)
                .setViewsEnabled(false)
                .validate())
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("bigquery.views-enabled config property must be enabled when skipping view materialization");
    }
}
