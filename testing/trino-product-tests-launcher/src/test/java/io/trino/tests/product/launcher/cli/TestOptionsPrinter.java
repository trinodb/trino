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
package io.trino.tests.product.launcher.cli;

import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

import java.util.List;
import java.util.Optional;

import static java.util.Collections.emptyList;
import static org.assertj.core.api.Assertions.assertThat;

public class TestOptionsPrinter
{
    @Test
    public void shouldFormatOptions()
    {
        assertThat(OptionsPrinter.format(new Options("test", false, emptyList()))).isEqualTo("--value test");
        assertThat(OptionsPrinter.format(new Options("test", true, emptyList()))).isEqualTo("--value test \\\n--boolean");
    }

    @Test
    public void shouldSkipNullStrings()
    {
        assertThat(OptionsPrinter.format(new Options("", false, emptyList()))).isEqualTo("");
    }

    @Test
    public void shouldSkipEmptyArguments()
    {
        assertThat(OptionsPrinter.format(new Options("", false, emptyList()))).isEqualTo("");
    }

    @Test
    public void shouldFormatArguments()
    {
        assertThat(OptionsPrinter.format(new Options("", false, ImmutableList.of("hello", "world")))).isEqualTo("-- hello world");
    }

    @Test
    public void shouldFormatMultipleObjects()
    {
        Options first = new Options("first", false, ImmutableList.of("hello", "world"));
        Options second = new Options("second", false, ImmutableList.of("hello", "world"));

        assertThat(OptionsPrinter.format(first, second)).isEqualTo("--value first \\\n-- hello world \\\n--value second \\\n-- hello world");
    }

    @Test
    public void shouldFormatNegatableOptions()
    {
        assertThat(OptionsPrinter.format(new NegatableOptions(true))).isEqualTo("--negatable");
        assertThat(OptionsPrinter.format(new NegatableOptions(false))).isEqualTo("--no-negatable");
    }

    @Test
    public void shouldFormatOptionalValues()
    {
        assertThat(OptionsPrinter.format(new OptionalFields(""))).isEqualTo("");
        assertThat(OptionsPrinter.format(new OptionalFields("test"))).isEqualTo("--value test");
        assertThat(OptionsPrinter.format(new OptionalFields(null))).isEqualTo("");
    }

    @SuppressWarnings("UnusedVariable") // these fields are used for validation testing
    private static class Options
    {
        @Option(names = "--value", paramLabel = "<value>", description = "Test value")
        public String value;

        @Option(names = "--boolean", paramLabel = "<boolean>", description = "Test value boolean")
        public boolean valueBoolean;

        @Parameters
        public List<String> arguments;

        public Options(String value, boolean valueBoolean, List<String> arguments)
        {
            this.value = value;
            this.valueBoolean = valueBoolean;
            this.arguments = arguments;
        }
    }

    @SuppressWarnings("UnusedVariable") // these fields are used for validation testing
    private static class NegatableOptions
    {
        @Option(names = "--no-negatable", paramLabel = "<boolean>", description = "Test value boolean", negatable = true)
        public boolean valueBoolean;

        public NegatableOptions(boolean valueBoolean)
        {
            this.valueBoolean = valueBoolean;
        }
    }

    @SuppressWarnings("UnusedVariable") // these fields are used for validation testing
    private static class OptionalFields
    {
        @Option(names = "--value", paramLabel = "<value>", description = "Test optional value")
        public Optional<String> value;

        @Option(names = "--hidden", hidden = true)
        public String hiddenOption = "hidden-value";

        public OptionalFields(String value)
        {
            this.value = Optional.ofNullable(value);
        }
    }
}
