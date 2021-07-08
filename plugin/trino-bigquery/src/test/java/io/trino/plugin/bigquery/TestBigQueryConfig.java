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
import io.airlift.configuration.ConfigurationFactory;
import io.airlift.units.Duration;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;

import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.testng.Assert.assertEquals;

public class TestBigQueryConfig
{
    @Test
    public void testDefaults()
    {
        BigQueryConfig config = new BigQueryConfig()
                .setCredentialsKey("key")
                .setCredentialsFile("cfile")
                .setProjectId("pid")
                .setParentProjectId("ppid")
                .setParallelism(20)
                .setViewMaterializationProject("vmproject")
                .setViewMaterializationDataset("vmdataset")
                .setMaxReadRowsRetries(10)
                .setCaseInsensitiveNameMatching(false)
                .setCaseInsensitiveNameMatchingCacheTtl(new Duration(1, MINUTES))
                .setViewsCacheTtl(new Duration(15, MINUTES));

        assertEquals(config.getCredentialsKey(), Optional.of("key"));
        assertEquals(config.getCredentialsFile(), Optional.of("cfile"));
        assertEquals(config.getProjectId(), Optional.of("pid"));
        assertEquals(config.getParentProjectId(), Optional.of("ppid"));
        assertEquals(config.getParallelism(), OptionalInt.of(20));
        assertEquals(config.getViewMaterializationProject(), Optional.of("vmproject"));
        assertEquals(config.getViewMaterializationDataset(), Optional.of("vmdataset"));
        assertEquals(config.getMaxReadRowsRetries(), 10);
        assertEquals(config.isCaseInsensitiveNameMatching(), false);
        assertEquals(config.getCaseInsensitiveNameMatchingCacheTtl(), new Duration(1, MINUTES));
        assertEquals(config.getViewsCacheTtl(), new Duration(15, MINUTES));
    }

    @Test
    public void testExplicitPropertyMappingsWithCredentialsKey()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("bigquery.credentials-key", "key")
                .put("bigquery.project-id", "pid")
                .put("bigquery.parent-project-id", "ppid")
                .put("bigquery.parallelism", "20")
                .put("bigquery.views-enabled", "true")
                .put("bigquery.view-materialization-project", "vmproject")
                .put("bigquery.view-materialization-dataset", "vmdataset")
                .put("bigquery.max-read-rows-retries", "10")
                .put("bigquery.case-insensitive-name-matching", "true")
                .put("bigquery.case-insensitive-name-matching.cache-ttl", "1s")
                .put("bigquery.views-cache-ttl", "1m")
                .build();

        ConfigurationFactory configurationFactory = new ConfigurationFactory(properties);
        BigQueryConfig config = configurationFactory.build(BigQueryConfig.class);

        assertEquals(config.getCredentialsKey(), Optional.of("key"));
        assertEquals(config.getProjectId(), Optional.of("pid"));
        assertEquals(config.getParentProjectId(), Optional.of("ppid"));
        assertEquals(config.getParallelism(), OptionalInt.of(20));
        assertEquals(config.isViewsEnabled(), true);
        assertEquals(config.getViewMaterializationProject(), Optional.of("vmproject"));
        assertEquals(config.getViewMaterializationDataset(), Optional.of("vmdataset"));
        assertEquals(config.getMaxReadRowsRetries(), 10);
        assertEquals(config.isCaseInsensitiveNameMatching(), true);
        assertEquals(config.getCaseInsensitiveNameMatchingCacheTtl(), new Duration(1, SECONDS));
        assertEquals(config.getViewsCacheTtl(), new Duration(1, MINUTES));
    }

    @Test
    public void testExplicitPropertyMappingsWithCredentialsFile()
            throws IOException
    {
        Path credentialsFile = Files.createTempFile(null, null);

        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("bigquery.credentials-file", credentialsFile.toString())
                .build();

        ConfigurationFactory configurationFactory = new ConfigurationFactory(properties);
        BigQueryConfig config = configurationFactory.build(BigQueryConfig.class);

        assertEquals(config.getCredentialsFile(), Optional.of(credentialsFile.toString()));
    }
}
