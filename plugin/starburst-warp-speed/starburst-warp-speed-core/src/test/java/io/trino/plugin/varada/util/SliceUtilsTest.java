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
package io.trino.plugin.varada.util;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.TrinoException;
import io.trino.spi.type.CharType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import org.junit.jupiter.api.Test;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.function.Function;

import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class SliceUtilsTest
{
    @Test
    public void testPrefix()
    {
        String prefixLow = "abc";
        String prefixHigh = "abd";
        String value = "abc123";
        long low = SliceUtils.str2int(Slices.wrappedBuffer(prefixLow.getBytes(Charset.defaultCharset())), true);
        long high = SliceUtils.str2int(Slices.wrappedBuffer(prefixHigh.getBytes(Charset.defaultCharset())), true);
        long valueLong = SliceUtils.str2int(Slices.wrappedBuffer(value.getBytes(Charset.defaultCharset())), true);

        assertThat(valueLong).isLessThan(high);
        assertThat(valueLong).isGreaterThan(low);
    }

    @Test
    public void testPrefixLargerThen8()
    {
        String prefixLow = "abc12345678";
        String prefixHigh = "abc12345679";
        String value = "abc12345678";
        long low = SliceUtils.str2int(Slices.wrappedBuffer(prefixLow.getBytes(Charset.defaultCharset())), true);
        long high = SliceUtils.str2int(Slices.wrappedBuffer(prefixHigh.getBytes(Charset.defaultCharset())), true);
        long valueLong = SliceUtils.str2int(Slices.wrappedBuffer(value.getBytes(Charset.defaultCharset())), true);

        assertThat(valueLong).isEqualTo(high);
        assertThat(valueLong).isEqualTo(low);
    }

    @Test
    public void testPrefixLengthEqualsTo8()
    {
        String prefixLow = "01234567";
        long low = SliceUtils.str2int(Slices.wrappedBuffer(prefixLow.getBytes(Charset.defaultCharset())), true);
        assertThat(Long.toHexString(low)).isEqualTo("3031323334353637");
    }

    @Test
    public void testPrefixCaseSensitive()
    {
        String lowerCase = "abc";
        String upperCase = "ABC";
        long low = SliceUtils.str2int(Slices.wrappedBuffer(lowerCase.getBytes(Charset.defaultCharset())), true);
        long upper = SliceUtils.str2int(Slices.wrappedBuffer(upperCase.getBytes(Charset.defaultCharset())), true);
        assertThat(low).isNotEqualTo(upper);
    }

    @Test
    public void testSuffix()
    {
        String suffixLow = "abc";
        String suffixHigh = "bbc";
        String value = "123abc";
        long low = SliceUtils.str2int(Slices.wrappedBuffer(suffixLow.getBytes(Charset.defaultCharset())), false);
        long high = SliceUtils.str2int(Slices.wrappedBuffer(suffixHigh.getBytes(Charset.defaultCharset())), false);
        long valueLong = SliceUtils.str2int(Slices.wrappedBuffer(value.getBytes(Charset.defaultCharset())), false);

        assertThat(low).isLessThan(high);
        assertThat(valueLong).isLessThan(high);
        assertThat(valueLong).isGreaterThan(low);
    }

    @Test
    public void testSuffixLongThen8AreEquals()
    {
        String suffixLow = "1234567890";
        String suffixHigh = "2234567890";
        long low = SliceUtils.str2int(Slices.wrappedBuffer(suffixLow.getBytes(Charset.defaultCharset())), false);
        long high = SliceUtils.str2int(Slices.wrappedBuffer(suffixHigh.getBytes(Charset.defaultCharset())), false);

        assertThat(low).isEqualTo(high);
    }

    @Test
    public void testSuffixLargerThen8()
    {
        String suffixLow = "abc12345678";
        String suffixHigh = "bbc12345678";
        String value = "123abc12345678";
        long low = SliceUtils.str2int(Slices.wrappedBuffer(suffixLow.getBytes(Charset.defaultCharset())), false);
        long high = SliceUtils.str2int(Slices.wrappedBuffer(suffixHigh.getBytes(Charset.defaultCharset())), false);
        long valueLong = SliceUtils.str2int(Slices.wrappedBuffer(value.getBytes(Charset.defaultCharset())), false);

        assertThat(valueLong).isEqualTo(high);
        assertThat(valueLong).isEqualTo(low);
    }

    @Test
    public void testGetSliceConverterLongVarCharShouldDoNothing()
    {
        Function<Slice, Slice> sliceConverter = SliceUtils.getSliceConverter(VarcharType.VARCHAR,
                16,
                false,
                true);
        byte[] buf = new byte[11];
        buf[10] = 1;
        Slice origBuffer = Slices.wrappedBuffer(buf);
        Slice convertedResult = sliceConverter.apply(origBuffer);
        assertThat(convertedResult).isSameAs(origBuffer);
    }

    @Test
    public void testGetSliceConverterShortVarCharShouldCopyValue()
    {
        Function<Slice, Slice> sliceConverter = SliceUtils.getSliceConverter(VarcharType.VARCHAR,
                6,
                true,
                true);
        byte[] origValue = new byte[] {'a', 'b', 'c', 'd', 'e'};
        Slice origBuffer = Slices.wrappedBuffer(origValue);
        Slice convertedResult = sliceConverter.apply(origBuffer);
        assertThat(convertedResult).isNotSameAs(origBuffer);
        assertThat(convertedResult.getBytes()).containsAnyOf(origValue);
    }

    @Test
    public void testGetSliceConverterShortVarCharSameLengthAsOriginShouldReturnSameSlice()
    {
        Function<Slice, Slice> sliceConverter = SliceUtils.getSliceConverter(VarcharType.VARCHAR,
                5,
                true,
                true);
        byte[] origValue = new byte[] {'a', 'b', 'c', 'd', 'e'};
        Slice origBuffer = Slices.wrappedBuffer(origValue);
        Slice convertedResult = sliceConverter.apply(origBuffer);
        assertThat(convertedResult).isSameAs(origBuffer);
    }

    @Test
    public void testGetSliceConverterCharShouldRemoveTrailingSpaces()
    {
        Function<Slice, Slice> sliceConverter = SliceUtils.getSliceConverter(CharType.createCharType(5),
                5,
                true,
                true);
        byte[] origValue = new byte[] {'a', 'b', 'c', 'd', ' '};
        Slice origBuffer = Slices.wrappedBuffer(origValue);
        Slice convertedResult = sliceConverter.apply(origBuffer);
        byte[] expected = new byte[] {'a', 'b', 'c', 'd', 0};
        assertThat(convertedResult.getBytes()).containsExactly(expected);
    }

    @Test
    public void testGetSliceConverterFixedSizeCharValidateSize()
    {
        Type type = CharType.createCharType(5);
        Function<Slice, Slice> sliceConverter = SliceUtils.getSliceConverter(type,
                3,
                true,
                true);
        byte[] origValue = new byte[] {'a', 'b', 'c', 'd'};
        Slice origBuffer = Slices.wrappedBuffer(origValue);
        assertThatThrownBy(() -> sliceConverter.apply(origBuffer))
                .isInstanceOf(TrinoException.class)
                .hasMessage(format("Mismatch in slice length[%d] and expected col length[%d], col type[%s]",
                        origBuffer.length(), 3, type));
    }

    @Test
    public void testGetSliceConverterVarCharValidateSize()
    {
        Type type = CharType.createCharType(5);
        Function<Slice, Slice> sliceConverter = SliceUtils.getSliceConverter(type,
                3,
                false,
                true);
        byte[] origValue = new byte[] {'a', 'b', 'c', 'd'};
        Slice origBuffer = Slices.wrappedBuffer(origValue);
        assertThatThrownBy(() -> sliceConverter.apply(origBuffer))
                .isInstanceOf(TrinoException.class)
                .hasMessage(format("Mismatch in slice length[%d] and expected col length[%d], col type[%s]",
                        origBuffer.length(), 3, type));
    }

    @Test
    public void testGetSliceConverterUTF8VarCharValidateSize()
    {
        Type type = CharType.createCharType(1);
        Function<Slice, Slice> sliceConverter = SliceUtils.getSliceConverter(type,
                1,
                false,
                true);
        byte[] origValue = "É".getBytes(StandardCharsets.UTF_8);
        Slice origBuffer = Slices.wrappedBuffer(origValue);
        assertThatThrownBy(() -> sliceConverter.apply(origBuffer))
                .isInstanceOf(TrinoException.class)
                .hasMessage(format("Mismatch in slice length[%d] and expected col length[%d], col type[%s]",
                        origBuffer.length(), 1, type));
    }

    @Test
    public void testGetSliceConverterVarChar()
    {
        Function<Slice, Slice> sliceConverter = SliceUtils.getSliceConverter(CharType.createCharType(1),
                1,
                false,
                true);
        byte[] origValue = "A".getBytes(StandardCharsets.US_ASCII);
        Slice origSlice = Slices.wrappedBuffer(origValue);
        assertThat(sliceConverter.apply(origSlice)).isEqualTo(origSlice);
    }
}
