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
package io.trino.plugin.redshift;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;

public class TestRedshiftConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(RedshiftConfig.class)
                .setFetchSize(null)
                .setUnloadLocation(null)
                .setUnloadOptions(null)
                .setIamRole(null));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("redshift.fetch-size", "2000")
                .put("redshift.unload-location", "s3://invalid")
                .put("redshift.unload-options", "ENCRYPTED")
                .put("redshift.iam-role", "arn:aws:iam::123456789000:role/redshift_iam_role")
                .buildOrThrow();

        RedshiftConfig expected = new RedshiftConfig()
                .setFetchSize(2000)
                .setUnloadLocation("s3://invalid")
                .setUnloadOptions("ENCRYPTED")
                .setIamRole("arn:aws:iam::123456789000:role/redshift_iam_role");

        assertFullMapping(properties, expected);
    }
}
