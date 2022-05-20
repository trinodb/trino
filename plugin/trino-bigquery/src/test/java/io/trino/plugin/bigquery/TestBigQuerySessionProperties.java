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

import com.google.cloud.bigquery.JobInfo;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Map;

import static io.trino.testing.SessionPropertiesUtil.assertDefaultProperties;
import static io.trino.testing.SessionPropertiesUtil.assertExplicitProperties;

public class TestBigQuerySessionProperties
{
    @Test
    public void testDefaults()
    {
        BigQuerySessionProperties provider = new BigQuerySessionProperties(new BigQueryConfig());

        Map<String, Object> properties = ImmutableMap.<String, Object>builder()
                .put("skip_view_materialization", false)
                .put("query_results_cache_enabled", false)
                .put("create_disposition_type", JobInfo.CreateDisposition.CREATE_IF_NEEDED)
                .buildOrThrow();

        assertDefaultProperties(provider.getSessionProperties(), properties);
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        BigQuerySessionProperties provider = new BigQuerySessionProperties(new BigQueryConfig());

        Map<String, Object> properties = ImmutableMap.<String, Object>builder()
                .put("skip_view_materialization", true)
                .put("query_results_cache_enabled", true)
                .put("create_disposition_type", JobInfo.CreateDisposition.CREATE_NEVER)
                .buildOrThrow();

        assertExplicitProperties(provider.getSessionProperties(), properties);
    }
}
