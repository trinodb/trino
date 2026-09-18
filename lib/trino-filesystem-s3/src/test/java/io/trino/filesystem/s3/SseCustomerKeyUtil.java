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
package io.trino.filesystem.s3;

import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Request;

import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;

import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Base64;
import java.util.function.Function;

final class SseCustomerKeyUtil
{
    private SseCustomerKeyUtil() {}

    static S3SseCustomerKey generateCustomerKey()
    {
        try {
            KeyGenerator keyGenerator = KeyGenerator.getInstance("AES");
            keyGenerator.init(256, new SecureRandom());
            SecretKey secretKey = keyGenerator.generateKey();
            String encodedKey = Base64.getEncoder().encodeToString(secretKey.getEncoded());
            return S3SseCustomerKey.onAes256(encodedKey);
        }
        catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }

    @SuppressWarnings("unchecked")
    static <T extends S3Request, V> V invokeOperationWithCustomerKey(T request, Function<T, V> operation, S3SseCustomerKey customerKey)
    {
        return switch (request) {
            case PutObjectRequest putObjectRequest -> operation.apply((T) putObjectRequest.toBuilder()
                    .sseCustomerAlgorithm(customerKey.algorithm())
                    .sseCustomerKey(customerKey.key())
                    .sseCustomerKeyMD5(customerKey.md5())
                    .build());
            case GetObjectRequest getObjectRequest -> operation.apply((T) getObjectRequest.toBuilder()
                    .sseCustomerAlgorithm(customerKey.algorithm())
                    .sseCustomerKey(customerKey.key())
                    .sseCustomerKeyMD5(customerKey.md5())
                    .build());
            default -> operation.apply(request);
        };
    }
}
