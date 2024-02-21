/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.schema.discovery.formats.csv;

import com.univocity.parsers.csv.CsvFormat;
import org.junit.jupiter.api.Test;

import static io.starburst.schema.discovery.Util.testFile;
import static io.starburst.schema.discovery.formats.csv.CheckCsvFormat.hasQuotedFields;
import static org.assertj.core.api.Assertions.assertThat;

public class TestHasQuotedValues
{
    @Test
    public void testQuotedFields()
    {
        assertThat(hasQuotedFields(testFile("csv/quoted/all-quoted.csv"), new CsvFormat())).isTrue();
        assertThat(hasQuotedFields(testFile("csv/quoted/last-quoted.csv"), new CsvFormat())).isTrue();
        assertThat(hasQuotedFields(testFile("csv/quoted/blank.csv"), new CsvFormat())).isFalse();
        assertThat(hasQuotedFields(testFile("csv/quoted/random.csv"), new CsvFormat())).isFalse();
    }
}
