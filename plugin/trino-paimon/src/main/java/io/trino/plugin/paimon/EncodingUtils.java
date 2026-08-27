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
import static java.util.Objects.requireNonNull;

public class EncodingUtils
{
    private static final Base64.Encoder BASE64_ENCODER = Base64.getUrlEncoder().withoutPadding();
    private static final Base64.Decoder BASE64_DECODER = Base64.getUrlDecoder();

    private EncodingUtils() {}

    public static <T> String encodeObjectToString(T object)
    {
        requireNonNull(object, "object is null");
        try {
            byte[] bytes = InstantiationUtil.serializeObject(object);
            return new String(BASE64_ENCODER.encode(bytes), UTF_8);
        }
        catch (Exception e) {
            throw new IllegalArgumentException(
                    "Failed to serialize object of type " + object.getClass().getName(), e);
        }
    }

    public static <T> T decodeStringToObject(String encodedStr)
    {
        requireNonNull(encodedStr, "encodedStr is null");
        final byte[] bytes;
        try {
            bytes = BASE64_DECODER.decode(encodedStr.getBytes(UTF_8));
        }
        catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Encoded string is not valid URL-safe Base64", e);
        }
        try {
            return InstantiationUtil.deserializeObject(bytes, EncodingUtils.class.getClassLoader());
        }
        catch (Exception e) {
            throw new IllegalArgumentException("Encoded string does not contain a serialized Java object", e);
        }
    }
}
