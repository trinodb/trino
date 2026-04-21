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

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;

import java.math.RoundingMode;

import static io.trino.plugin.jdbc.DecimalSessionSessionProperties.DECIMAL_MAPPING;
import static java.math.RoundingMode.UNNECESSARY;

/**
 * A {@link DecimalConfig} implementation to be used when mapping to NUMBER is not supported or not default behavior.
 *
 * @deprecated This class is to be removed once all connectors are updated to map to NUMBER by default.
 */
@Deprecated
public class LegacyDecimalConfig
        implements DecimalConfig
{
    private DecimalMapping decimalMapping = DecimalMapping.STRICT;
    private int decimalDefaultScale;
    private RoundingMode decimalRoundingMode = UNNECESSARY;

    @NotNull
    @Override
    public DecimalMapping getDecimalMapping()
    {
        return decimalMapping;
    }

    @Config("decimal-mapping")
    @ConfigDescription("Decimal mapping for unspecified and exceeding precision decimals. STRICT skips them. MAP_TO_NUMBER maps them to Trino NUMBER. ALLOW_OVERFLOW requires setting proper decimal scale and rounding mode")
    public LegacyDecimalConfig setDecimalMapping(DecimalMapping decimalMapping)
    {
        this.decimalMapping = decimalMapping;
        return this;
    }

    @Min(0)
    @Max(38)
    @Override
    public int getDecimalDefaultScale()
    {
        return decimalDefaultScale;
    }

    @Config("decimal-default-scale")
    @ConfigDescription("Default decimal scale for mapping unspecified and exceeding precision decimals. Not used when " + DECIMAL_MAPPING + " is set to STRICT or MAP_TO_NUMBER")
    public LegacyDecimalConfig setDecimalDefaultScale(Integer decimalDefaultScale)
    {
        this.decimalDefaultScale = decimalDefaultScale;
        return this;
    }

    @NotNull
    @Override
    public RoundingMode getDecimalRoundingMode()
    {
        return decimalRoundingMode;
    }

    @Config("decimal-rounding-mode")
    @ConfigDescription("Rounding mode for mapping unspecified and exceeding precision decimals. Not used when " + DECIMAL_MAPPING + " is set to STRICT or MAP_TO_NUMBER")
    public LegacyDecimalConfig setDecimalRoundingMode(RoundingMode decimalRoundingMode)
    {
        this.decimalRoundingMode = decimalRoundingMode;
        return this;
    }
}
