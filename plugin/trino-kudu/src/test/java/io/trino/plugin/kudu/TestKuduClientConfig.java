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
package io.trino.plugin.kudu;

import com.google.common.collect.ImmutableMap;
import io.airlift.units.Duration;
import org.testng.annotations.Test;

import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;

public class TestKuduClientConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(KuduClientConfig.class)
                .setMasterAddresses("")
                .setDefaultAdminOperationTimeout(new Duration(30, SECONDS))
                .setDefaultOperationTimeout(new Duration(30, SECONDS))
                .setDisableStatistics(false)
                .setSchemaEmulationEnabled(false)
                .setSchemaEmulationPrefix("presto::")
                .setDynamicFilteringWaitTimeout(new Duration(0, MINUTES))
                .setAllowLocalScheduling(false));
    }

    @Test
    public void testExplicitPropertyMappingsWithCredentialsKey()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("kudu.client.master-addresses", "localhost")
                .put("kudu.client.default-admin-operation-timeout", "1m")
                .put("kudu.client.default-operation-timeout", "5m")
                .put("kudu.client.disable-statistics", "true")
                .put("kudu.schema-emulation.enabled", "true")
                .put("kudu.schema-emulation.prefix", "trino::")
                .put("kudu.dynamic-filtering.wait-timeout", "30m")
                .put("kudu.allow-local-scheduling", "true")
                .buildOrThrow();

        KuduClientConfig expected = new KuduClientConfig()
                .setMasterAddresses("localhost")
                .setDefaultAdminOperationTimeout(new Duration(1, MINUTES))
                .setDefaultOperationTimeout(new Duration(5, MINUTES))
                .setDisableStatistics(true)
                .setSchemaEmulationEnabled(true)
                .setSchemaEmulationPrefix("trino::")
                .setDynamicFilteringWaitTimeout(new Duration(30, MINUTES))
                .setAllowLocalScheduling(true);

        assertFullMapping(properties, expected);
    }
}
