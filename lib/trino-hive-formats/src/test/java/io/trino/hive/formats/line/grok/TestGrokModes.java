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
package io.trino.hive.formats.line.grok;

import com.google.common.collect.ImmutableList;
import io.trino.hive.formats.line.Column;
import io.trino.hive.formats.line.grok.exception.GrokException;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Optional;

import static io.trino.hive.formats.line.grok.TestGrokUtils.assertError;
import static io.trino.hive.formats.line.grok.TestGrokUtils.assertLine;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test different modes of grok
 *    - strict mode:
 *            no automate data type conversion
 *            throw exceptions when data type conversion fails
 *    - default mode (non-strict): do automate data type conversion, always convert to string when data type conversion fails
 *            do automate data type conversion when no data type assigned
 *            always convert to string when data type conversion fails
 *   -  named only mode:
 *            only translate named fields, unnamed fields are null and padded at the end
 *            (i.e. if input.format = "%{WORD: word} %{NUMBER}" then only word is captured
 *   - null on parse error mode:
 *            return null on parse error
 *            throw exceptions on parse error
 */
public class TestGrokModes
{
    @Test
    public void test001_AutomaticDataTypeConversion()
            throws GrokException
    {
        String log = "123 06-21-2015 this is a strict mode";
        String logFormat = "%{INT:intNum} %{DATE_US:dateStr} %{GREEDYDATA:message}";

        // strict mode - never do automatic data type conversion
        Grok strictGrok = Grok.create();
        strictGrok.setStrictMode(true);
        strictGrok.compile(logFormat);
        Match strictMatch = strictGrok.match(log);
        strictMatch.captures();

        assertThat(strictMatch.toMap().get("intNum").getClass()).isEqualTo(String.class);
        assertThat(strictMatch.toMap().get("dateStr").getClass()).isEqualTo(String.class);
        assertThat(strictMatch.toMap().get("message").getClass()).isEqualTo(String.class);

        // default mode - do automatic data type conversion
        Grok defaultGrok = Grok.create();
        defaultGrok.compile(logFormat);
        Match defaultMatch = defaultGrok.match(log);
        defaultMatch.captures();

        assertThat(defaultMatch.toMap().get("intNum").getClass()).isEqualTo(Integer.class);
        assertThat(defaultMatch.toMap().get("dateStr").getClass()).isEqualTo(Date.class);
        assertThat(defaultMatch.toMap().get("message").getClass()).isEqualTo(String.class);
    }

    @Test
    public void test002_DataTypeConversionFailure()
            throws GrokException
    {
        String[] errlogs = {"123.4", "06-21-2015 23:12:56"};
        String[] logFormats = {"^%{NUMBER:test:int}$", "%{DATE_US:test:datetime:MM/dd/yy HHmmss}"};

        // strict mode - throw exceptions when data type conversion fails
        boolean[] throwns = {false, false};
        Grok strictGrok = Grok.create();
        strictGrok.setStrictMode(true);
        for (int i = 0; i < 2; i++) {
            strictGrok.compile(logFormats[i]);
            Match strictMatch = strictGrok.match(errlogs[i]);
            try {
                strictMatch.captures();
            }
            catch (GrokException e) {
                throwns[i] = true;
            }
            assertThat(throwns[i]).isTrue();
        }

        // default mode - convert to string when data type conversion fails
        Grok defaultGrok = Grok.create();
        for (int i = 0; i < 2; i++) {
            defaultGrok.compile(logFormats[i]);
            Match defaultMatch = defaultGrok.match(errlogs[i]);
            defaultMatch.captures();
            assertThat(defaultMatch.toMap().get("test").getClass()).isEqualTo(String.class);
        }
    }

    @Test
    public void test003_NamedOnlyMode()
            throws IOException
    {
        String log = "hello 123";
        String inputFormat = "%{WORD} %{NUMBER}";
        List<Column> columns = ImmutableList.of(new Column("a", VARCHAR, 0), new Column("b", VARCHAR, 1));

        assertLine(
                columns,
                log,
                inputFormat,
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Arrays.asList(null, null));

        assertLine(
                columns,
                log,
                inputFormat,
                Optional.empty(),
                Optional.empty(),
                Optional.of("false"),
                Optional.empty(),
                Arrays.asList("hello", "123"));

        String inputFormatMixedNamed = "%{WORD} %{NUMBER:number}";
        assertLine(
                columns,
                log,
                inputFormatMixedNamed,
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Arrays.asList("123", null));
    }

    @Test
    public void test004_NullOnParseErrorMode()
            throws IOException
    {
        String log = "abc";
        String inputFormat = "%{WORD:word}";
        List<Column> columns = ImmutableList.of(new Column("a", INTEGER, 0));

        assertError(
                columns,
                log,
                Optional.of(inputFormat),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                "Error Parsing a column in the table: For input string: \"abc\"",
                Optional.of(NumberFormatException.class));

        assertLine(
                columns,
                log,
                inputFormat,
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.of("true"),
                Arrays.asList((Object) null));
    }
}
