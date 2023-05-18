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

public class TestColorFunctions
{
    @Test
    public void testParseRgb()
    {
        assertThat(parseRgb(utf8Slice("#000"))).isEqualTo(0x00_00_00);
        assertThat(parseRgb(utf8Slice("#FFF"))).isEqualTo(0xFF_FF_FF);
        assertThat(parseRgb(utf8Slice("#F00"))).isEqualTo(0xFF_00_00);
        assertThat(parseRgb(utf8Slice("#0F0"))).isEqualTo(0x00_FF_00);
        assertThat(parseRgb(utf8Slice("#00F"))).isEqualTo(0x00_00_FF);
        assertThat(parseRgb(utf8Slice("#700"))).isEqualTo(0x77_00_00);
        assertThat(parseRgb(utf8Slice("#070"))).isEqualTo(0x00_77_00);
        assertThat(parseRgb(utf8Slice("#007"))).isEqualTo(0x00_00_77);

        assertThat(parseRgb(utf8Slice("#cde"))).isEqualTo(0xCC_DD_EE);
    }

    @Test
    public void testGetComponent()
    {
        assertThat(getRed(parseRgb(utf8Slice("#789")))).isEqualTo(0x77);
        assertThat(getGreen(parseRgb(utf8Slice("#789")))).isEqualTo(0x88);
        assertThat(getBlue(parseRgb(utf8Slice("#789")))).isEqualTo(0x99);
    }

    @Test
    public void testToRgb()
    {
        assertThat(rgb(0xFF, 0, 0)).isEqualTo(0xFF_00_00);
        assertThat(rgb(0, 0xFF, 0)).isEqualTo(0x00_FF_00);
        assertThat(rgb(0, 0, 0xFF)).isEqualTo(0x00_00_FF);
    }

    @Test
    public void testColor()
    {
        assertThat(color(utf8Slice("black"))).isEqualTo(-1);
        assertThat(color(utf8Slice("red"))).isEqualTo(-2);
        assertThat(color(utf8Slice("green"))).isEqualTo(-3);
        assertThat(color(utf8Slice("yellow"))).isEqualTo(-4);
        assertThat(color(utf8Slice("blue"))).isEqualTo(-5);
        assertThat(color(utf8Slice("magenta"))).isEqualTo(-6);
        assertThat(color(utf8Slice("cyan"))).isEqualTo(-7);
        assertThat(color(utf8Slice("white"))).isEqualTo(-8);

        assertThat(color(utf8Slice("#f00"))).isEqualTo(0xFF_00_00);
        assertThat(color(utf8Slice("#0f0"))).isEqualTo(0x00_FF_00);
        assertThat(color(utf8Slice("#00f"))).isEqualTo(0x00_00_FF);
    }

    @Test
    public void testBar()
    {
        assertThat(bar(0.6, 5, color(utf8Slice("#f0f")), color(utf8Slice("#00f")))).isEqualTo(utf8Slice("\u001B[38;5;201m\u2588\u001B[38;5;165m\u2588\u001B[38;5;129m\u2588\u001B[0m  "));

        assertThat(bar(1, 10, color(utf8Slice("#f00")), color(utf8Slice("#0f0")))).isEqualTo(utf8Slice("\u001B[38;5;196m\u2588\u001B[38;5;202m\u2588\u001B[38;5;208m\u2588\u001B[38;5;214m\u2588\u001B[38;5;226m\u2588\u001B[38;5;226m\u2588\u001B[38;5;154m\u2588\u001B[38;5;118m\u2588\u001B[38;5;82m\u2588\u001B[38;5;46m\u2588\u001B[0m"));

        assertThat(bar(0.6, 5, color(utf8Slice("#f0f")), color(utf8Slice("#00f")))).isEqualTo(utf8Slice("\u001B[38;5;201m\u2588\u001B[38;5;165m\u2588\u001B[38;5;129m\u2588\u001B[0m  "));
    }

    @Test
    public void testRenderBoolean()
    {
        assertThat(render(true)).isEqualTo(utf8Slice("\u001b[38;5;2m✓\u001b[0m"));
        assertThat(render(false)).isEqualTo(utf8Slice("\u001b[38;5;1m✗\u001b[0m"));
    }

    @Test
    public void testRenderString()
    {
        assertThat(render(utf8Slice("hello"), color(utf8Slice("red")))).isEqualTo(utf8Slice("\u001b[38;5;1mhello\u001b[0m"));

        assertThat(render(utf8Slice("hello"), color(utf8Slice("#f00")))).isEqualTo(utf8Slice("\u001b[38;5;196mhello\u001b[0m"));
        assertThat(render(utf8Slice("hello"), color(utf8Slice("#0f0")))).isEqualTo(utf8Slice("\u001b[38;5;46mhello\u001b[0m"));
        assertThat(render(utf8Slice("hello"), color(utf8Slice("#00f")))).isEqualTo(utf8Slice("\u001b[38;5;21mhello\u001b[0m"));
    }

    @Test
    public void testRenderLong()
    {
        assertThat(render(1234, color(utf8Slice("red")))).isEqualTo(utf8Slice("\u001b[38;5;1m1234\u001b[0m"));

        assertThat(render(1234, color(utf8Slice("#f00")))).isEqualTo(utf8Slice("\u001b[38;5;196m1234\u001b[0m"));
        assertThat(render(1234, color(utf8Slice("#0f0")))).isEqualTo(utf8Slice("\u001b[38;5;46m1234\u001b[0m"));
        assertThat(render(1234, color(utf8Slice("#00f")))).isEqualTo(utf8Slice("\u001b[38;5;21m1234\u001b[0m"));
    }

    @Test
    public void testRenderDouble()
    {
        assertThat(render(1234.5678, color(utf8Slice("red")))).isEqualTo(utf8Slice("\u001b[38;5;1m1234.5678\u001b[0m"));
        assertThat(render(1234.5678f, color(utf8Slice("red")))).isEqualTo(utf8Slice(format("\u001b[38;5;1m%s\u001b[0m", (double) 1234.5678f)));

        assertThat(render(1234.5678, color(utf8Slice("#f00")))).isEqualTo(utf8Slice("\u001b[38;5;196m1234.5678\u001b[0m"));
        assertThat(render(1234.5678, color(utf8Slice("#0f0")))).isEqualTo(utf8Slice("\u001b[38;5;46m1234.5678\u001b[0m"));
        assertThat(render(1234.5678, color(utf8Slice("#00f")))).isEqualTo(utf8Slice("\u001b[38;5;21m1234.5678\u001b[0m"));
    }

    @Test
    public void testInterpolate()
    {
        assertThat(color(0, 0, 255, color(utf8Slice("#000")), color(utf8Slice("#fff")))).isEqualTo(0x00_00_00);
        assertThat(color(0.0f, 0.0f, 255.0f, color(utf8Slice("#000")), color(utf8Slice("#fff")))).isEqualTo(0x00_00_00);
        assertThat(color(128, 0, 255, color(utf8Slice("#000")), color(utf8Slice("#fff")))).isEqualTo(0x80_80_80);
        assertThat(color(255, 0, 255, color(utf8Slice("#000")), color(utf8Slice("#fff")))).isEqualTo(0xFF_FF_FF);

        assertThat(color(-1, 42, 52, rgb(0xFF, 0, 0), rgb(0xFF, 0xFF, 0))).isEqualTo(0xFF_00_00);
        assertThat(color(47, 42, 52, rgb(0xFF, 0, 0), rgb(0xFF, 0xFF, 0))).isEqualTo(0xFF_80_00);
        assertThat(color(142, 42, 52, rgb(0xFF, 0, 0), rgb(0xFF, 0xFF, 0))).isEqualTo(0xFF_FF_00);

        assertThat(color(-42, color(utf8Slice("#000")), color(utf8Slice("#fff")))).isEqualTo(0x00_00_00);
        assertThat(color(0.0, color(utf8Slice("#000")), color(utf8Slice("#fff")))).isEqualTo(0x00_00_00);
        assertThat(color(0.5, color(utf8Slice("#000")), color(utf8Slice("#fff")))).isEqualTo(0x80_80_80);
        assertThat(color(1.0, color(utf8Slice("#000")), color(utf8Slice("#fff")))).isEqualTo(0xFF_FF_FF);
        assertThat(color(42, color(utf8Slice("#000")), color(utf8Slice("#fff")))).isEqualTo(0xFF_FF_FF);
        assertThat(color(1.0f, color(utf8Slice("#000")), color(utf8Slice("#fff")))).isEqualTo(0xFF_FF_FF);
        assertThat(color(-0.0f, color(utf8Slice("#000")), color(utf8Slice("#fff")))).isEqualTo(0x00_00_00);
        assertThat(color(0.0f, color(utf8Slice("#000")), color(utf8Slice("#fff")))).isEqualTo(0x00_00_00);
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
