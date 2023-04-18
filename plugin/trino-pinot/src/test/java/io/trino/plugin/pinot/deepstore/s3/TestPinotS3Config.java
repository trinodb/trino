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
package io.trino.plugin.pinot.deepstore.s3;

import com.google.common.collect.ImmutableMap;
import io.airlift.configuration.testing.ConfigAssertions;
import org.testng.annotations.Test;

import java.util.Map;

public class TestPinotS3Config
{
    @Test
    public void testDefaults()
    {
        ConfigAssertions.assertRecordedDefaults(
                ConfigAssertions.recordDefaults(PinotS3Config.class)
                        .setS3AccessKeyFile(null)
                        .setS3SecretKeyFile(null)
                        .setS3Endpoint(null)
                        .setS3Region(null));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("pinot.s3-accesskey-file", "/my-access-key-file")
                .put("pinot.s3-secretkey-file", "/my-secret-key-file")
                .put("pinot.s3-endpoint", "http://my-s3-endpoint")
                .put("pinot.s3-region", "my-region")
                .buildOrThrow();

        PinotS3Config expected = new PinotS3Config()
                .setS3AccessKeyFile("/my-access-key-file")
                .setS3SecretKeyFile("/my-secret-key-file")
                .setS3Endpoint("http://my-s3-endpoint")
                .setS3Region("my-region");
        ConfigAssertions.assertFullMapping(properties, expected);
    }
}
