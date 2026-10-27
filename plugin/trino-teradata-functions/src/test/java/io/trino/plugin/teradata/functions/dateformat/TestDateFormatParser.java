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
package io.trino.plugin.teradata.functions.dateformat;

import io.trino.plugin.teradata.functions.dateformat.DateFormatParser.Formatter;
import io.trino.spi.TrinoException;
import org.antlr.v4.runtime.Token;
import org.junit.jupiter.api.Test;

import java.time.DateTimeException;
import java.time.LocalDateTime;
import java.util.stream.Collectors;

import static java.time.ZoneOffset.UTC;
import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestDateFormatParser
{
    @Test
    public void testTokenize()
    {
        assertThat(DateFormatParser.tokenize("yyyy mm").stream().map(Token::getType).collect(Collectors.toList()))
                .isEqualTo(asList(DateFormat.YYYY, DateFormat.TEXT, DateFormat.MM));
    }

    @Test
    public void testGreedinessLongFirst()
    {
        assertThat(DateFormatParser.tokenize("yy")).hasSize(1);
        assertThat(DateFormatParser.tokenize("yyyy")).hasSize(1);
        assertThat(DateFormatParser.tokenize("yyyyyy")).hasSize(2);
    }

    @Test
    public void testInvalidTokenTokenize()
    {
        assertThat(DateFormatParser.tokenize("ala").stream().map(Token::getType).collect(Collectors.toList()))
                .isEqualTo(asList(DateFormat.UNRECOGNIZED, DateFormat.UNRECOGNIZED, DateFormat.UNRECOGNIZED));
    }

    @Test
    public void testInvalidTokenCreate1()
    {
        assertThatThrownBy(() -> DateFormatParser.createDateTimeFormatter("ala"))
                .isInstanceOf(TrinoException.class);
    }

    @Test
    public void testInvalidTokenCreate2()
    {
        assertThatThrownBy(() -> DateFormatParser.createDateTimeFormatter("yyym/mm/dd"))
                .isInstanceOf(TrinoException.class);
    }

    @Test
    public void testCreateDateTimeFormatter()
    {
        Formatter formatter = DateFormatParser.createDateTimeFormatter("yyyy/mm/dd");
        assertThat(formatter.parseMillis("1988/04/08")).isEqualTo(LocalDateTime.of(1988, 4, 8, 0, 0).toInstant(UTC).toEpochMilli());
    }

    @Test
    public void testPartialDatesAndTimes()
    {
        assertThat(DateFormatParser.createDateTimeFormatter("mm/dd").parseMillis("2/29"))
                .isEqualTo(LocalDateTime.of(2000, 2, 29, 0, 0).toInstant(UTC).toEpochMilli());
        assertThat(DateFormatParser.createDateTimeFormatter("hh24:mi:ss").parseMillis("2:3:4"))
                .isEqualTo(LocalDateTime.of(1970, 1, 1, 2, 3, 4).toInstant(UTC).toEpochMilli());
        assertThat(DateFormatParser.createDateTimeFormatter("yy/mm/dd").parseMillis("00/1/1"))
                .isEqualTo(LocalDateTime.of(2000, 1, 1, 0, 0).toInstant(UTC).toEpochMilli());
        assertThat(DateFormatParser.createDateTimeFormatter("yy/mm/dd").parseMillis("99/1/1"))
                .isEqualTo(LocalDateTime.of(2099, 1, 1, 0, 0).toInstant(UTC).toEpochMilli());
        assertThat(DateFormatParser.createDateTimeFormatter("yyyy/mm/dd").parseMillis("019/02/01"))
                .isEqualTo(LocalDateTime.of(19, 2, 1, 0, 0).toInstant(UTC).toEpochMilli());
        assertThat(DateFormatParser.createDateTimeFormatter("yyyy/mm/dd").parseMillis("+19/02/01"))
                .isEqualTo(LocalDateTime.of(19, 2, 1, 0, 0).toInstant(UTC).toEpochMilli());
    }

    @Test
    public void testInvalidDates()
    {
        assertThatThrownBy(() -> DateFormatParser.createDateTimeFormatter("yyyy/mm/dd").parseMillis("2019/2/29"))
                .isInstanceOf(DateTimeException.class);
        assertThatThrownBy(() -> DateFormatParser.createDateTimeFormatter("yyyy/mm/dd").parseMillis("2019/002/01"))
                .isInstanceOf(DateTimeException.class);
        assertThatThrownBy(() -> DateFormatParser.createDateTimeFormatter("yyyymmdd").parseMillis("198841"))
                .isInstanceOf(DateTimeException.class);
    }
}
