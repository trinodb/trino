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
package io.trino.operator.scalar;

import io.trino.sql.query.QueryAssertions;
import org.testng.annotations.Test;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.operator.scalar.ColorFunctions.bar;
import static io.trino.operator.scalar.ColorFunctions.color;
import static io.trino.operator.scalar.ColorFunctions.getBlue;
import static io.trino.operator.scalar.ColorFunctions.getGreen;
import static io.trino.operator.scalar.ColorFunctions.getRed;
import static io.trino.operator.scalar.ColorFunctions.parseRgb;
import static io.trino.operator.scalar.ColorFunctions.render;
import static io.trino.operator.scalar.ColorFunctions.rgb;
import static io.trino.spi.function.OperatorType.INDETERMINATE;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertEquals;

public class TestColorFunctions
{
    @Test
    public void testParseRgb()
    {
        assertEquals(parseRgb(utf8Slice("#000")), 0x00_00_00);
        assertEquals(parseRgb(utf8Slice("#FFF")), 0xFF_FF_FF);
        assertEquals(parseRgb(utf8Slice("#F00")), 0xFF_00_00);
        assertEquals(parseRgb(utf8Slice("#0F0")), 0x00_FF_00);
        assertEquals(parseRgb(utf8Slice("#00F")), 0x00_00_FF);
        assertEquals(parseRgb(utf8Slice("#700")), 0x77_00_00);
        assertEquals(parseRgb(utf8Slice("#070")), 0x00_77_00);
        assertEquals(parseRgb(utf8Slice("#007")), 0x00_00_77);

        assertEquals(parseRgb(utf8Slice("#cde")), 0xCC_DD_EE);
    }

    @Test
    public void testGetComponent()
    {
        assertEquals(getRed(parseRgb(utf8Slice("#789"))), 0x77);
        assertEquals(getGreen(parseRgb(utf8Slice("#789"))), 0x88);
        assertEquals(getBlue(parseRgb(utf8Slice("#789"))), 0x99);
    }

    @Test
    public void testToRgb()
    {
        assertEquals(rgb(0xFF, 0, 0), 0xFF_00_00);
        assertEquals(rgb(0, 0xFF, 0), 0x00_FF_00);
        assertEquals(rgb(0, 0, 0xFF), 0x00_00_FF);
    }

    @Test
    public void testColor()
    {
        assertEquals(color(utf8Slice("black")), -1);
        assertEquals(color(utf8Slice("red")), -2);
        assertEquals(color(utf8Slice("green")), -3);
        assertEquals(color(utf8Slice("yellow")), -4);
        assertEquals(color(utf8Slice("blue")), -5);
        assertEquals(color(utf8Slice("magenta")), -6);
        assertEquals(color(utf8Slice("cyan")), -7);
        assertEquals(color(utf8Slice("white")), -8);

        assertEquals(color(utf8Slice("#f00")), 0xFF_00_00);
        assertEquals(color(utf8Slice("#0f0")), 0x00_FF_00);
        assertEquals(color(utf8Slice("#00f")), 0x00_00_FF);
    }

    @Test
    public void testBar()
    {
        assertEquals(bar(0.6, 5, color(utf8Slice("#f0f")), color(utf8Slice("#00f"))),
                utf8Slice("\u001B[38;5;201m\u2588\u001B[38;5;165m\u2588\u001B[38;5;129m\u2588\u001B[0m  "));

        assertEquals(bar(1, 10, color(utf8Slice("#f00")), color(utf8Slice("#0f0"))),
                utf8Slice("\u001B[38;5;196m\u2588\u001B[38;5;202m\u2588\u001B[38;5;208m\u2588\u001B[38;5;214m\u2588\u001B[38;5;226m\u2588\u001B[38;5;226m\u2588\u001B[38;5;154m\u2588\u001B[38;5;118m\u2588\u001B[38;5;82m\u2588\u001B[38;5;46m\u2588\u001B[0m"));

        assertEquals(bar(0.6, 5, color(utf8Slice("#f0f")), color(utf8Slice("#00f"))),
                utf8Slice("\u001B[38;5;201m\u2588\u001B[38;5;165m\u2588\u001B[38;5;129m\u2588\u001B[0m  "));
    }

    @Test
    public void testRenderBoolean()
    {
        assertEquals(render(true), utf8Slice("\u001b[38;5;2m✓\u001b[0m"));
        assertEquals(render(false), utf8Slice("\u001b[38;5;1m✗\u001b[0m"));
    }

    @Test
    public void testRenderString()
    {
        assertEquals(render(utf8Slice("hello"), color(utf8Slice("red"))), utf8Slice("\u001b[38;5;1mhello\u001b[0m"));

        assertEquals(render(utf8Slice("hello"), color(utf8Slice("#f00"))), utf8Slice("\u001b[38;5;196mhello\u001b[0m"));
        assertEquals(render(utf8Slice("hello"), color(utf8Slice("#0f0"))), utf8Slice("\u001b[38;5;46mhello\u001b[0m"));
        assertEquals(render(utf8Slice("hello"), color(utf8Slice("#00f"))), utf8Slice("\u001b[38;5;21mhello\u001b[0m"));
    }

    @Test
    public void testRenderLong()
    {
        assertEquals(render(1234, color(utf8Slice("red"))), utf8Slice("\u001b[38;5;1m1234\u001b[0m"));

        assertEquals(render(1234, color(utf8Slice("#f00"))), utf8Slice("\u001b[38;5;196m1234\u001b[0m"));
        assertEquals(render(1234, color(utf8Slice("#0f0"))), utf8Slice("\u001b[38;5;46m1234\u001b[0m"));
        assertEquals(render(1234, color(utf8Slice("#00f"))), utf8Slice("\u001b[38;5;21m1234\u001b[0m"));
    }

    @Test
    public void testRenderDouble()
    {
        assertEquals(render(1234.5678, color(utf8Slice("red"))), utf8Slice("\u001b[38;5;1m1234.5678\u001b[0m"));
        assertEquals(render(1234.5678f, color(utf8Slice("red"))), utf8Slice(format("\u001b[38;5;1m%s\u001b[0m", (double) 1234.5678f)));

        assertEquals(render(1234.5678, color(utf8Slice("#f00"))), utf8Slice("\u001b[38;5;196m1234.5678\u001b[0m"));
        assertEquals(render(1234.5678, color(utf8Slice("#0f0"))), utf8Slice("\u001b[38;5;46m1234.5678\u001b[0m"));
        assertEquals(render(1234.5678, color(utf8Slice("#00f"))), utf8Slice("\u001b[38;5;21m1234.5678\u001b[0m"));
    }

    @Test
    public void testInterpolate()
    {
        assertEquals(color(0, 0, 255, color(utf8Slice("#000")), color(utf8Slice("#fff"))), 0x00_00_00);
        assertEquals(color(0.0f, 0.0f, 255.0f, color(utf8Slice("#000")), color(utf8Slice("#fff"))), 0x00_00_00);
        assertEquals(color(128, 0, 255, color(utf8Slice("#000")), color(utf8Slice("#fff"))), 0x80_80_80);
        assertEquals(color(255, 0, 255, color(utf8Slice("#000")), color(utf8Slice("#fff"))), 0xFF_FF_FF);

        assertEquals(color(-1, 42, 52, rgb(0xFF, 0, 0), rgb(0xFF, 0xFF, 0)), 0xFF_00_00);
        assertEquals(color(47, 42, 52, rgb(0xFF, 0, 0), rgb(0xFF, 0xFF, 0)), 0xFF_80_00);
        assertEquals(color(142, 42, 52, rgb(0xFF, 0, 0), rgb(0xFF, 0xFF, 0)), 0xFF_FF_00);

        assertEquals(color(-42, color(utf8Slice("#000")), color(utf8Slice("#fff"))), 0x00_00_00);
        assertEquals(color(0.0, color(utf8Slice("#000")), color(utf8Slice("#fff"))), 0x00_00_00);
        assertEquals(color(0.5, color(utf8Slice("#000")), color(utf8Slice("#fff"))), 0x80_80_80);
        assertEquals(color(1.0, color(utf8Slice("#000")), color(utf8Slice("#fff"))), 0xFF_FF_FF);
        assertEquals(color(42, color(utf8Slice("#000")), color(utf8Slice("#fff"))), 0xFF_FF_FF);
        assertEquals(color(1.0f, color(utf8Slice("#000")), color(utf8Slice("#fff"))), 0xFF_FF_FF);
        assertEquals(color(-0.0f, color(utf8Slice("#000")), color(utf8Slice("#fff"))), 0x00_00_00);
        assertEquals(color(0.0f, color(utf8Slice("#000")), color(utf8Slice("#fff"))), 0x00_00_00);
    }

    @Test
    public void testIndeterminate()
    {
        try (QueryAssertions assertions = new QueryAssertions()) {
            assertThat(assertions.operator(INDETERMINATE, "color(null)"))
                    .isEqualTo(true);

            assertThat(assertions.operator(INDETERMINATE, "color('black')"))
                    .isEqualTo(false);
        }
    }
}
