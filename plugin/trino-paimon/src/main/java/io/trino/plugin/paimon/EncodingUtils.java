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
package io.trino.plugin.paimon;

import org.apache.paimon.utils.InstantiationUtil;

import java.util.Base64;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Utils for encoding.
 */
public class EncodingUtils
{
    private EncodingUtils() {}

    private static final Base64.Encoder BASE64_ENCODER = Base64.getUrlEncoder().withoutPadding();

    private static final Base64.Decoder BASE64_DECODER = Base64.getUrlDecoder();

    public static <T> String encodeObjectToString(T t)
    {
        try {
            byte[] bytes = InstantiationUtil.serializeObject(t);
            return new String(BASE64_ENCODER.encode(bytes), UTF_8);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static <T> T decodeStringToObject(String encodedStr)
    {
        final byte[] bytes = BASE64_DECODER.decode(encodedStr.getBytes(UTF_8));
        try {
            return InstantiationUtil.deserializeObject(bytes, EncodingUtils.class.getClassLoader());
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
