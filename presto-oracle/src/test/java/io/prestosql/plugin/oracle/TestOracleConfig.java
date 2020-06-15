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
package io.prestosql.plugin.oracle;

import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import javax.validation.constraints.Max;
import javax.validation.constraints.Min;

import java.math.RoundingMode;
import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static io.airlift.testing.ValidationAssertions.assertFailsValidation;

public class TestOracleConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(OracleConfig.class)
                .setSynonymsEnabled(false)
                .setDefaultNumberScale(null)
                .setNumberRoundingMode(RoundingMode.UNNECESSARY)
                .setFetchSize(1000));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("oracle.synonyms.enabled", "true")
                .put("oracle.number.default-scale", "2")
                .put("oracle.number.rounding-mode", "CEILING")
                .put("oracle.fetch-size", "5000")
                .build();

        OracleConfig expected = new OracleConfig()
                .setSynonymsEnabled(true)
                .setDefaultNumberScale(2)
                .setFetchSize(5000)
                .setNumberRoundingMode(RoundingMode.CEILING);

        assertFullMapping(properties, expected);
    }

    @Test
    public void testValidation()
    {
        assertFailsValidation(
                new OracleConfig()
                        .setDefaultNumberScale(-1),
                "defaultNumberScale",
                "must be greater than or equal to 0",
                Min.class);

        assertFailsValidation(
                new OracleConfig()
                        .setDefaultNumberScale(39),
                "defaultNumberScale",
                "must be less than or equal to 38",
                Max.class);

        assertFailsValidation(
                new OracleConfig()
                        .setFetchSize(0),
                "fetchSize",
                "must be greater than or equal to 1",
                Min.class);
    }
}
