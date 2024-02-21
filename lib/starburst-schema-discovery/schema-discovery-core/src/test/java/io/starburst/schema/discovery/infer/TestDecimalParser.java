/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.schema.discovery.infer;

import io.starburst.schema.discovery.options.OptionsMap;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.text.DecimalFormat;
import java.util.Locale;

import static org.assertj.core.api.Assertions.assertThat;

public class TestDecimalParser
{
    @Test
    public void testDecimalParser()
    {
        OptionsMap optionsMap = new OptionsMap();
        DecimalFormat decimalFormat = optionsMap.decimalFormat(Locale.ROOT);
        assertThat(DecimalParser.parse(decimalFormat, "1.0")).contains(BigDecimal.valueOf(1.0));
        assertThat(DecimalParser.parse(decimalFormat, "NaN")).isEmpty();
        assertThat(DecimalParser.parse(decimalFormat, Double.toString(Double.POSITIVE_INFINITY))).isEmpty();
        assertThat(DecimalParser.parse(decimalFormat, Double.toString(Double.NEGATIVE_INFINITY))).isEmpty();
    }
}
