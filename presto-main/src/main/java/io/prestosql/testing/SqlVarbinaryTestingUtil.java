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
package io.prestosql.testing;

import com.google.common.primitives.UnsignedBytes;
import io.prestosql.spi.type.SqlVarbinary;

import static com.google.common.base.CharMatcher.ascii;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.io.BaseEncoding.base16;
import static java.nio.charset.StandardCharsets.ISO_8859_1;
import static java.nio.charset.StandardCharsets.US_ASCII;

public final class SqlVarbinaryTestingUtil
{
    private SqlVarbinaryTestingUtil() {}

    public static SqlVarbinary sqlVarbinaryFromHex(String value)
    {
        return new SqlVarbinary(base16().decode(value));
    }

    public static SqlVarbinary sqlVarbinary(String value)
    {
        checkArgument(ascii().matchesAllOf(value), "string must be ASCII: '%s'", value);
        return new SqlVarbinary(value.getBytes(US_ASCII));
    }

    public static SqlVarbinary sqlVarbinaryFromIso(String value)
    {
        return new SqlVarbinary(value.getBytes(ISO_8859_1));
    }

    public static SqlVarbinary sqlVarbinary(int... bytesAsInts)
    {
        byte[] bytes = new byte[bytesAsInts.length];
        for (int i = 0; i < bytes.length; i++) {
            bytes[i] = UnsignedBytes.checkedCast(bytesAsInts[i]);
        }
        return new SqlVarbinary(bytes);
    }
}
