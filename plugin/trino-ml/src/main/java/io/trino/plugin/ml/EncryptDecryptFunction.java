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
import cn.hutool.crypto.SecureUtil;
import cn.hutool.crypto.SmUtil;
import cn.hutool.crypto.asymmetric.KeyType;
import cn.hutool.crypto.asymmetric.RSA;
import cn.hutool.crypto.asymmetric.SM2;
import cn.hutool.crypto.symmetric.AES;
import cn.hutool.crypto.symmetric.DES;
import cn.hutool.crypto.symmetric.SymmetricCrypto;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.function.Description;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlNullable;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.StandardTypes;
import org.bouncycastle.util.Strings;

public final class EncryptDecryptFunction {
    @ScalarFunction("sm4")
    @Description("SM4 encryption requires filling in encrypted data, encryption key, and encryption character encoding")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice sm4Encrypt(
            @SqlType(StandardTypes.VARCHAR) Slice text,
            @SqlNullable @SqlType("varchar(16)") Slice key,
            @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice charset) {
        try {
            SymmetricCrypto sm4 = SmUtil.sm4(key.getBytes());
            return Slices.utf8Slice(sm4.encryptHex(sliceToString(text), sliceToString(charset)));
        } catch (Exception e) {
            throw new RuntimeException("加密失败,检查SM4密钥是否正确.");
        }
    }

    @ScalarFunction("from_sm4")
    @Description("SM4 decryption requires filling in decryption data, decryption key, and decryption character encoding")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice sm4Decrypt(
            @SqlType(StandardTypes.VARCHAR) Slice text,
            @SqlNullable @SqlType("varchar(16)") Slice key,
            @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice charset) {
        try {
            SymmetricCrypto sm4 = SmUtil.sm4(key.getBytes());
            return Slices.utf8Slice(sm4.decryptStr(sliceToString(text), CharsetUtil.parse(sliceToString(charset))));
        } catch (Exception e) {
            throw new RuntimeException("解密失败,检查SM4密钥是否正确.");
        }
    }


    @ScalarFunction("sm2")
    @Description("SM2 encryption, passing in the data to be encrypted and the public key of the hexadecimal string")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice sm2Encrypt(
            @SqlType(StandardTypes.VARCHAR) Slice text,
            @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice hexPublicKey) {
        try {
            SM2 sm2 = SmUtil.sm2(null, sliceToString(hexPublicKey));
            return Slices.utf8Slice(sm2.encryptBase64(text.getBytes(), KeyType.PublicKey));
        } catch (Exception e) {
            throw new RuntimeException("加密失败,检查SM2公钥是否正确.");
        }
    }

    @ScalarFunction("from_sm2")
    @Description("SM2 decryption, passing in the data to be decrypted and the private key of the hexadecimal string")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice sm2Decrypt(
            @SqlType(StandardTypes.VARCHAR) Slice text,
            @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice hexPrivateKey) {
        try {
            SM2 sm2 = SmUtil.sm2(sliceToString(hexPrivateKey), null);
            return Slices.utf8Slice(sm2.decryptStr(sliceToString(text), KeyType.PrivateKey));
        } catch (Exception e) {
            throw new RuntimeException("解密失败,检查SM2私钥是否正确.");
        }
    }

    @ScalarFunction("rsa")
    @Description("rsa encryption")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice rsaEncrypt(
            @SqlType(StandardTypes.VARCHAR) Slice text,
            @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice publicKey) {
        try {
            RSA rsa = new RSA(null, sliceToString(publicKey));
            return Slices.utf8Slice(rsa.encryptBase64(sliceToString(text), KeyType.PublicKey));
        } catch (Exception e) {
            throw new RuntimeException("加密失败,检查RSA公钥是否正确.");
        }
    }

    @ScalarFunction("from_rsa")
    @Description("rsa decryption")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice rsaDecrypt(
            @SqlType(StandardTypes.VARCHAR) Slice text,
            @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice privateKey) {
        try {
            RSA rsa = new RSA(sliceToString(privateKey), null);
            return Slices.utf8Slice(rsa.decryptStr(sliceToString(text), KeyType.PrivateKey));
        } catch (Exception e) {
            throw new RuntimeException("解密失败,检查RSA私钥是否正确.");
        }
    }


    @ScalarFunction("aes")
    @Description("aes encryption")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice aesEncrypt(
            @SqlType(StandardTypes.VARCHAR) Slice text,
            @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice key) {
        try {
            AES aes = SecureUtil.aes(key.getBytes());
            return Slices.utf8Slice(aes.encryptHex(sliceToString(text)));
        } catch (Exception e) {
            throw new RuntimeException("加密失败,检查AES密钥是否正确.");
        }
    }

    @ScalarFunction("from_aes")
    @Description("rsa decryption")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice aesDecrypt(
            @SqlType(StandardTypes.VARCHAR) Slice text,
            @SqlNullable @SqlType("varchar(16)") Slice key) {
        try {
            AES aes = SecureUtil.aes(key.getBytes());
            return Slices.utf8Slice(aes.decryptStr(sliceToString(text), CharsetUtil.CHARSET_UTF_8));
        } catch (Exception e) {
            throw new RuntimeException("解密失败,检查AES密钥是否正确.");
        }
    }

    @ScalarFunction("des")
    @Description("aes encryption")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice desEncrypt(
            @SqlType(StandardTypes.VARCHAR) Slice text,
            @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice key) {
        try {
            DES des = SecureUtil.des(key.getBytes());
            return Slices.utf8Slice(des.encryptHex(sliceToString(text)));
        } catch (Exception e) {
            throw new RuntimeException("加密失败,检查DES密钥是否正确.");
        }
    }

    @ScalarFunction("from_des")
    @Description("rsa decryption")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice desDecrypt(
            @SqlType(StandardTypes.VARCHAR) Slice text,
            @SqlNullable @SqlType("varchar(16)") Slice key) {
        try {
            DES des = SecureUtil.des(key.getBytes());
            return Slices.utf8Slice(des.decryptStr(sliceToString(text), CharsetUtil.CHARSET_UTF_8));
        } catch (Exception e) {
            throw new RuntimeException("解密失败,检查DES密钥是否正确.");
        }
    }


    @ScalarFunction("shift_left")
    @Description("向左偏移函数:shiftLeft")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice shiftLeft(
            @SqlType(StandardTypes.VARCHAR) Slice text,
            @SqlType(StandardTypes.BIGINT) long offset) {
        return Slices.utf8Slice(shiftLeft(sliceToString(text), (int) offset));
    }

    @ScalarFunction("shift_right")
    @Description("向右偏移函数:shiftRight")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice shiftRight(
            @SqlType(StandardTypes.VARCHAR) Slice text,
            @SqlType(StandardTypes.BIGINT) long offset) {
        return Slices.utf8Slice(shiftRight(sliceToString(text), (int) offset));
    }


    public static String shiftLeft(String str, int offset) {
        offset = offset % str.length();
        return str.substring(offset) + str.substring(0, offset);
    }

    public static String shiftRight(String str, int offset) {
        offset = offset % str.length();
        return str.substring(str.length() - offset) + str.substring(0, str.length() - offset);
    }

    private static String sliceToString(Slice slice) {
        return Strings.fromUTF8ByteArray(slice.getBytes());
    }
}
