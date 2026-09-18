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
package io.trino.plugin.jdbc;

import com.google.inject.Module;
import io.airlift.bootstrap.Bootstrap;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static io.trino.plugin.jdbc.DecimalConfig.DecimalMapping.ALLOW_OVERFLOW;
import static io.trino.plugin.jdbc.DecimalConfig.DecimalMapping.MAP_TO_NUMBER;
import static io.trino.plugin.jdbc.DecimalConfig.DecimalMapping.STRICT;
import static io.trino.plugin.jdbc.DecimalModule.MappingToNumber.OFF_BY_DEFAULT;
import static io.trino.plugin.jdbc.DecimalModule.MappingToNumber.ON_BY_DEFAULT;
import static java.math.RoundingMode.HALF_UP;
import static java.math.RoundingMode.UNNECESSARY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestDecimalModule
{
    @Test
    void testLegacyDecimalModule()
    {
        DecimalModule decimalModule = new DecimalModule();

        // defaults
        DecimalConfig decimalConfig = configure(decimalModule, Map.of()).initialize()
                .getInstance(DecimalConfig.class);
        assertThat(decimalConfig.getDecimalMapping()).isEqualTo(STRICT);
        assertThat(decimalConfig.getDecimalDefaultScale()).isEqualTo(0);
        assertThat(decimalConfig.getDecimalRoundingMode()).isEqualTo(UNNECESSARY);

        // STRICT
        decimalConfig = configure(decimalModule, Map.of("decimal-mapping", "STRICT")).initialize()
                .getInstance(DecimalConfig.class);
        assertThat(decimalConfig.getDecimalMapping()).isEqualTo(STRICT);
        assertThat(decimalConfig.getDecimalDefaultScale()).isEqualTo(0);
        assertThat(decimalConfig.getDecimalRoundingMode()).isEqualTo(UNNECESSARY);

        // ALLOW_OVERFLOW
        decimalConfig = configure(decimalModule, Map.of("decimal-mapping", "ALLOW_OVERFLOW")).initialize()
                .getInstance(DecimalConfig.class);
        assertThat(decimalConfig.getDecimalMapping()).isEqualTo(ALLOW_OVERFLOW);
        assertThat(decimalConfig.getDecimalDefaultScale()).isEqualTo(0);
        assertThat(decimalConfig.getDecimalRoundingMode()).isEqualTo(UNNECESSARY);

        // MAP_TO_NUMBER
        assertThatThrownBy(configure(decimalModule, Map.of("decimal-mapping", "MAP_TO_NUMBER"))::initialize)
                .hasRootCauseInstanceOf(IllegalStateException.class)
                .hasRootCauseMessage("MAP_TO_NUMBER decimal mapping is not supported in this connector");

        // just decimal-default-scale: this configuration does not make much sense, but doesn't fail for backward compatibility reasons.
        decimalConfig = configure(decimalModule, Map.of("decimal-default-scale", "0")).initialize()
                .getInstance(DecimalConfig.class);
        assertThat(decimalConfig.getDecimalMapping()).isEqualTo(STRICT);
        assertThat(decimalConfig.getDecimalDefaultScale()).isEqualTo(0);
        decimalConfig = configure(decimalModule, Map.of("decimal-default-scale", "5")).initialize()
                .getInstance(DecimalConfig.class);
        assertThat(decimalConfig.getDecimalMapping()).isEqualTo(STRICT);
        assertThat(decimalConfig.getDecimalDefaultScale()).isEqualTo(5);

        // just decimal-rounding-mode: this configuration does not make much sense, but doesn't fail for backward compatibility reasons.
        decimalConfig = configure(decimalModule, Map.of("decimal-rounding-mode", "UNNECESSARY")).initialize()
                .getInstance(DecimalConfig.class);
        assertThat(decimalConfig.getDecimalMapping()).isEqualTo(STRICT);
        assertThat(decimalConfig.getDecimalRoundingMode()).isEqualTo(UNNECESSARY);
        decimalConfig = configure(decimalModule, Map.of("decimal-rounding-mode", "HALF_UP")).initialize()
                .getInstance(DecimalConfig.class);
        assertThat(decimalConfig.getDecimalMapping()).isEqualTo(STRICT);
        assertThat(decimalConfig.getDecimalRoundingMode()).isEqualTo(HALF_UP);
    }

    @Test
    void testMapToNumberDisabled()
    {
        DecimalModule decimalModule = DecimalModule.withNumberMapping(OFF_BY_DEFAULT);

        // defaults
        DecimalConfig decimalConfig = configure(decimalModule, Map.of()).initialize()
                .getInstance(DecimalConfig.class);
        assertThat(decimalConfig.getDecimalMapping()).isEqualTo(STRICT);
        assertThat(decimalConfig.getDecimalDefaultScale()).isEqualTo(0);
        assertThat(decimalConfig.getDecimalRoundingMode()).isEqualTo(UNNECESSARY);

        // STRICT
        decimalConfig = configure(decimalModule, Map.of("decimal-mapping", "STRICT")).initialize()
                .getInstance(DecimalConfig.class);
        assertThat(decimalConfig.getDecimalMapping()).isEqualTo(STRICT);
        assertThat(decimalConfig.getDecimalDefaultScale()).isEqualTo(0);
        assertThat(decimalConfig.getDecimalRoundingMode()).isEqualTo(UNNECESSARY);

        // ALLOW_OVERFLOW
        decimalConfig = configure(decimalModule, Map.of("decimal-mapping", "ALLOW_OVERFLOW")).initialize()
                .getInstance(DecimalConfig.class);
        assertThat(decimalConfig.getDecimalMapping()).isEqualTo(ALLOW_OVERFLOW);
        assertThat(decimalConfig.getDecimalDefaultScale()).isEqualTo(0);
        assertThat(decimalConfig.getDecimalRoundingMode()).isEqualTo(UNNECESSARY);

        // MAP_TO_NUMBER
        decimalConfig = configure(decimalModule, Map.of("decimal-mapping", "MAP_TO_NUMBER")).initialize()
                .getInstance(DecimalConfig.class);
        assertThat(decimalConfig.getDecimalMapping()).isEqualTo(MAP_TO_NUMBER);
        assertThat(decimalConfig.getDecimalDefaultScale()).isEqualTo(0);
        assertThat(decimalConfig.getDecimalRoundingMode()).isEqualTo(UNNECESSARY);

        // MAP_TO_NUMBER and decimal-default-scale: this configuration does not make much sense, but doesn't fail for backward compatibility reasons.
        decimalConfig = configure(decimalModule, Map.of("decimal-mapping", "MAP_TO_NUMBER", "decimal-default-scale", "5")).initialize()
                .getInstance(DecimalConfig.class);
        assertThat(decimalConfig.getDecimalMapping()).isEqualTo(MAP_TO_NUMBER);
        assertThat(decimalConfig.getDecimalDefaultScale()).isEqualTo(5);
        assertThat(decimalConfig.getDecimalRoundingMode()).isEqualTo(UNNECESSARY);

        // MAP_TO_NUMBER and decimal-rounding-mode: this configuration does not make much sense, but doesn't fail for backward compatibility reasons.
        decimalConfig = configure(decimalModule, Map.of("decimal-mapping", "MAP_TO_NUMBER", "decimal-rounding-mode", "HALF_UP")).initialize()
                .getInstance(DecimalConfig.class);
        assertThat(decimalConfig.getDecimalMapping()).isEqualTo(MAP_TO_NUMBER);
        assertThat(decimalConfig.getDecimalDefaultScale()).isEqualTo(0);
        assertThat(decimalConfig.getDecimalRoundingMode()).isEqualTo(HALF_UP);

        // just decimal-default-scale: this configuration does not make much sense, but doesn't fail for backward compatibility reasons.
        decimalConfig = configure(decimalModule, Map.of("decimal-default-scale", "0")).initialize()
                .getInstance(DecimalConfig.class);
        assertThat(decimalConfig.getDecimalMapping()).isEqualTo(STRICT);
        assertThat(decimalConfig.getDecimalDefaultScale()).isEqualTo(0);
        decimalConfig = configure(decimalModule, Map.of("decimal-default-scale", "5")).initialize()
                .getInstance(DecimalConfig.class);
        assertThat(decimalConfig.getDecimalMapping()).isEqualTo(STRICT);
        assertThat(decimalConfig.getDecimalDefaultScale()).isEqualTo(5);

        // just decimal-rounding-mode: this configuration does not make much sense, but doesn't fail for backward compatibility reasons.
        decimalConfig = configure(decimalModule, Map.of("decimal-rounding-mode", "UNNECESSARY")).initialize()
                .getInstance(DecimalConfig.class);
        assertThat(decimalConfig.getDecimalMapping()).isEqualTo(STRICT);
        assertThat(decimalConfig.getDecimalRoundingMode()).isEqualTo(UNNECESSARY);
        decimalConfig = configure(decimalModule, Map.of("decimal-rounding-mode", "HALF_UP")).initialize()
                .getInstance(DecimalConfig.class);
        assertThat(decimalConfig.getDecimalMapping()).isEqualTo(STRICT);
        assertThat(decimalConfig.getDecimalRoundingMode()).isEqualTo(HALF_UP);
    }

    @Test
    void testMapToNumberEnabled()
    {
        DecimalModule decimalModule = DecimalModule.withNumberMapping(ON_BY_DEFAULT);

        // defaults
        DecimalConfig decimalConfig = configure(decimalModule, Map.of()).initialize()
                .getInstance(DecimalConfig.class);
        assertThat(decimalConfig.getDecimalMapping()).isEqualTo(MAP_TO_NUMBER);
        assertThat(decimalConfig.getDecimalDefaultScale()).isEqualTo(0);
        assertThat(decimalConfig.getDecimalRoundingMode()).isEqualTo(UNNECESSARY);

        // STRICT
        decimalConfig = configure(decimalModule, Map.of("decimal-mapping", "STRICT")).initialize()
                .getInstance(DecimalConfig.class);
        assertThat(decimalConfig.getDecimalMapping()).isEqualTo(STRICT);
        assertThat(decimalConfig.getDecimalDefaultScale()).isEqualTo(0);
        assertThat(decimalConfig.getDecimalRoundingMode()).isEqualTo(UNNECESSARY);

        // ALLOW_OVERFLOW
        decimalConfig = configure(decimalModule, Map.of("decimal-mapping", "ALLOW_OVERFLOW")).initialize()
                .getInstance(DecimalConfig.class);
        assertThat(decimalConfig.getDecimalMapping()).isEqualTo(ALLOW_OVERFLOW);
        assertThat(decimalConfig.getDecimalDefaultScale()).isEqualTo(0);
        assertThat(decimalConfig.getDecimalRoundingMode()).isEqualTo(UNNECESSARY);

        // MAP_TO_NUMBER
        decimalConfig = configure(decimalModule, Map.of("decimal-mapping", "MAP_TO_NUMBER")).initialize()
                .getInstance(DecimalConfig.class);
        assertThat(decimalConfig.getDecimalMapping()).isEqualTo(MAP_TO_NUMBER);
        assertThat(decimalConfig.getDecimalDefaultScale()).isEqualTo(0);
        assertThat(decimalConfig.getDecimalRoundingMode()).isEqualTo(UNNECESSARY);

        // MAP_TO_NUMBER and decimal-default-scale: this configuration does not make much sense, but doesn't fail for backward compatibility reasons.
        decimalConfig = configure(decimalModule, Map.of("decimal-mapping", "MAP_TO_NUMBER", "decimal-default-scale", "5")).initialize()
                .getInstance(DecimalConfig.class);
        assertThat(decimalConfig.getDecimalMapping()).isEqualTo(MAP_TO_NUMBER);
        assertThat(decimalConfig.getDecimalDefaultScale()).isEqualTo(5);
        assertThat(decimalConfig.getDecimalRoundingMode()).isEqualTo(UNNECESSARY);

        // MAP_TO_NUMBER and decimal-rounding-mode: this configuration does not make much sense, but doesn't fail for backward compatibility reasons.
        decimalConfig = configure(decimalModule, Map.of("decimal-mapping", "MAP_TO_NUMBER", "decimal-rounding-mode", "HALF_UP")).initialize()
                .getInstance(DecimalConfig.class);
        assertThat(decimalConfig.getDecimalMapping()).isEqualTo(MAP_TO_NUMBER);
        assertThat(decimalConfig.getDecimalDefaultScale()).isEqualTo(0);
        assertThat(decimalConfig.getDecimalRoundingMode()).isEqualTo(HALF_UP);

        // just decimal-default-scale: this configuration does not make much sense, but doesn't fail for backward compatibility reasons.
        decimalConfig = configure(decimalModule, Map.of("decimal-default-scale", "0")).initialize()
                .getInstance(DecimalConfig.class);
        assertThat(decimalConfig.getDecimalMapping()).isEqualTo(MAP_TO_NUMBER);
        assertThat(decimalConfig.getDecimalDefaultScale()).isEqualTo(0);
        decimalConfig = configure(decimalModule, Map.of("decimal-default-scale", "5")).initialize()
                .getInstance(DecimalConfig.class);
        assertThat(decimalConfig.getDecimalMapping()).isEqualTo(MAP_TO_NUMBER);
        assertThat(decimalConfig.getDecimalDefaultScale()).isEqualTo(5);

        // just decimal-rounding-mode: this configuration does not make much sense, but doesn't fail for backward compatibility reasons.
        decimalConfig = configure(decimalModule, Map.of("decimal-rounding-mode", "UNNECESSARY")).initialize()
                .getInstance(DecimalConfig.class);
        assertThat(decimalConfig.getDecimalMapping()).isEqualTo(MAP_TO_NUMBER);
        assertThat(decimalConfig.getDecimalRoundingMode()).isEqualTo(UNNECESSARY);
        decimalConfig = configure(decimalModule, Map.of("decimal-rounding-mode", "HALF_UP")).initialize()
                .getInstance(DecimalConfig.class);
        assertThat(decimalConfig.getDecimalMapping()).isEqualTo(MAP_TO_NUMBER);
        assertThat(decimalConfig.getDecimalRoundingMode()).isEqualTo(HALF_UP);
    }

    private static Bootstrap configure(Module module, Map<String, String> config)
    {
        return new Bootstrap(module)
                .setRequiredConfigurationProperties(config)
                .doNotInitializeLogging()
                .quiet();
    }
}
