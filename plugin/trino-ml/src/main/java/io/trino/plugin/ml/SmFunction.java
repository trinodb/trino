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
package io.trino.plugin.ml;

import cn.hutool.core.util.CharsetUtil;
import cn.hutool.core.util.HexUtil;
import cn.hutool.crypto.SmUtil;
import cn.hutool.crypto.asymmetric.KeyType;
import cn.hutool.crypto.asymmetric.SM2;
import cn.hutool.crypto.symmetric.SymmetricCrypto;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.function.Description;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlNullable;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.StandardTypes;
import org.bouncycastle.util.Strings;

public final class SmFunction {
    @ScalarFunction("sm4")
    @Description("SM4 encryption requires filling in encrypted data, encryption key, and encryption character encoding")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice sm4Encrypt(
            @SqlType(StandardTypes.VARCHAR) Slice text,
            @SqlNullable @SqlType("varchar(16)") Slice key,
            @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice charset) {
        SymmetricCrypto sm4 = SmUtil.sm4(key.getBytes());
        return Slices.utf8Slice(sm4.encryptHex(sliceToString(text), sliceToString(charset)));
    }

    @ScalarFunction("from_sm4")
    @Description("SM4 decryption requires filling in decryption data, decryption key, and decryption character encoding")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice sm4Decrypt(
            @SqlType(StandardTypes.VARCHAR) Slice text,
            @SqlNullable @SqlType("varchar(16)") Slice key,
            @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice charset) {
        SymmetricCrypto sm4 = SmUtil.sm4(key.getBytes());
        return Slices.utf8Slice(sm4.decryptStr(sliceToString(text), CharsetUtil.parse(sliceToString(charset))));
    }


    @ScalarFunction("sm2")
    @Description("SM2 encryption, passing in the data to be encrypted and the public key of the hexadecimal string")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice sm2Encrypt(
            @SqlType(StandardTypes.VARCHAR) Slice text,
            @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice hexPublicKey) {
        SM2 sm2 = SmUtil.sm2(null, sliceToString(hexPublicKey));
        return Slices.utf8Slice(sm2.encryptHex(text.getBytes(), KeyType.PublicKey));
    }

    @ScalarFunction("from_sm2")
    @Description("SM2 decryption, passing in the data to be decrypted and the private key of the hexadecimal string")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice sm2Decrypt(
            @SqlType(StandardTypes.VARCHAR) Slice text,
            @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice hexPrivateKey) {
        SM2 sm2 = SmUtil.sm2(sliceToString(hexPrivateKey), null);
        return Slices.utf8Slice(sm2.decryptStr(sliceToString(text), KeyType.PrivateKey));
    }


    private static String sliceToString(Slice slice) {
        return Strings.fromUTF8ByteArray(slice.getBytes());
    }
}
