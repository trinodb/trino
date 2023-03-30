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
package io.trino.plugin.oracle;

import com.google.common.collect.ImmutableMap;
import io.airlift.units.Duration;
import org.testng.annotations.Test;

import javax.validation.constraints.Max;
import javax.validation.constraints.Min;

import java.math.RoundingMode;
import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static io.airlift.testing.ValidationAssertions.assertFailsValidation;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;

public class TestOracleConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(OracleConfig.class)
                .setDisableAutomaticFetchSize(false)
                .setSynonymsEnabled(false)
                .setRemarksReportingEnabled(false)
                .setDefaultNumberScale(null)
                .setNumberRoundingMode(RoundingMode.UNNECESSARY)
                .setConnectionPoolEnabled(true)
                .setConnectionPoolMinSize(1)
                .setConnectionPoolMaxSize(30)
                .setInactiveConnectionTimeout(new Duration(20, MINUTES)));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("oracle.disable-automatic-fetch-size", "true")
                .put("oracle.synonyms.enabled", "true")
                .put("oracle.remarks-reporting.enabled", "true")
                .put("oracle.number.default-scale", "2")
                .put("oracle.number.rounding-mode", "CEILING")
                .put("oracle.connection-pool.enabled", "false")
                .put("oracle.connection-pool.min-size", "10")
                .put("oracle.connection-pool.max-size", "20")
                .put("oracle.connection-pool.inactive-timeout", "30s")
                .buildOrThrow();

        OracleConfig expected = new OracleConfig()
                .setDisableAutomaticFetchSize(true)
                .setSynonymsEnabled(true)
                .setRemarksReportingEnabled(true)
                .setDefaultNumberScale(2)
                .setNumberRoundingMode(RoundingMode.CEILING)
                .setConnectionPoolEnabled(false)
                .setConnectionPoolMinSize(10)
                .setConnectionPoolMaxSize(20)
                .setInactiveConnectionTimeout(new Duration(30, SECONDS));

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
                        .setConnectionPoolMinSize(-1),
                "connectionPoolMinSize",
                "must be greater than or equal to 0",
                Min.class);

        assertFailsValidation(
                new OracleConfig()
                        .setConnectionPoolMaxSize(0),
                "connectionPoolMaxSize",
                "must be greater than or equal to 1",
                Min.class);
    }
}
