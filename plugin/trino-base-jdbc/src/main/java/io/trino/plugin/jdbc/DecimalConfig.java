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

public class DecimalConfig
{
    private DecimalMapping decimalMapping = DecimalMapping.STRICT;
    private int decimalDefaultScale;
    private RoundingMode decimalRoundingMode = UNNECESSARY;

    public enum DecimalMapping
    {
        STRICT,
        ALLOW_OVERFLOW,
        /**/;
    }

    @NotNull
    public DecimalMapping getDecimalMapping()
    {
        return decimalMapping;
    }

    @Config("decimal-mapping")
    @ConfigDescription("Decimal mapping for unspecified and exceeding precision decimals. STRICT skips them. ALLOW_OVERFLOW requires setting proper decimal scale and rounding mode")
    public DecimalConfig setDecimalMapping(DecimalMapping decimalMapping)
    {
        this.decimalMapping = decimalMapping;
        return this;
    }

    @Min(0)
    @Max(38)
    public int getDecimalDefaultScale()
    {
        return decimalDefaultScale;
    }

    @Config("decimal-default-scale")
    @ConfigDescription("Default decimal scale for mapping unspecified and exceeding precision decimals. Not used when " + DECIMAL_MAPPING + " is set to STRICT")
    public DecimalConfig setDecimalDefaultScale(Integer decimalDefaultScale)
    {
        this.decimalDefaultScale = decimalDefaultScale;
        return this;
    }

    @NotNull
    public RoundingMode getDecimalRoundingMode()
    {
        return decimalRoundingMode;
    }

    @Config("decimal-rounding-mode")
    @ConfigDescription("Rounding mode for mapping unspecified and exceeding precision decimals. Not used when" + DECIMAL_MAPPING + "is set to STRICT")
    public DecimalConfig setDecimalRoundingMode(RoundingMode decimalRoundingMode)
    {
        this.decimalRoundingMode = decimalRoundingMode;
        return this;
    }
}
