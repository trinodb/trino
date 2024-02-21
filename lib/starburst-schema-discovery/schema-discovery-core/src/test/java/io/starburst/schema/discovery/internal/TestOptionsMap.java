/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.schema.discovery.internal;

import io.starburst.schema.discovery.TableChanges;
import io.starburst.schema.discovery.formats.csv.CsvOptions;
import io.starburst.schema.discovery.options.GeneralOptions;
import io.starburst.schema.discovery.options.OptionsMap;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static io.starburst.schema.discovery.models.LowerCaseString.toLowerCase;
import static org.assertj.core.api.Assertions.assertThat;

public class TestOptionsMap
{
    @Test
    public void testDefaults()
    {
        GeneralOptions generalOptions = new GeneralOptions(new OptionsMap());
        assertThat(generalOptions.forcedFormat()).isEmpty();
        assertThat(generalOptions.maxSampleTables()).isEqualTo(Integer.MAX_VALUE);
        assertThat(generalOptions.nanValue()).isEqualTo("NaN");
    }

    @Test
    public void testFileOverrides()
    {
        Map<String, String> options = new HashMap<>(CsvOptions.standard());
        options.put("s.f." + CsvOptions.DELIMITER, "-");
        CsvOptions csvOptions = new CsvOptions(new OptionsMap(options).withTableName(new TableChanges.TableName(Optional.of(toLowerCase("s")), toLowerCase("f"))));
        assertThat(csvOptions.delimiter()).isEqualTo("-");
    }
}
