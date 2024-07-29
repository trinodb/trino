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
package io.trino.filesystem.s3.ssec;

import io.trino.filesystem.s3.AbstractTestS3FileSystem;
import io.trino.filesystem.s3.S3SseCustomerKey;
import software.amazon.awssdk.services.s3.DelegatingS3Client;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Request;
import software.amazon.awssdk.utils.BinaryUtils;

import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;

import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.function.Function;

public abstract class AbstractTestS3FileSystemWithSseCustomerKey
        extends AbstractTestS3FileSystem
{
    static final String CUSTOMER_KEY = generateCustomerKey();
    protected S3SseCustomerKey s3SseCustomerKey;

    @Override
    protected void initEnvironment()
    {
        super.initEnvironment();
        s3SseCustomerKey = S3SseCustomerKey.onAes256(CUSTOMER_KEY);
    }

    private static String generateCustomerKey()
    {
        try {
            KeyGenerator keyGenerator = KeyGenerator.getInstance("AES");
            keyGenerator.init(256, new SecureRandom());
            SecretKey secretKey = keyGenerator.generateKey();
            return BinaryUtils.toBase64(secretKey.getEncoded());
        }
        catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected S3Client createS3Client()
    {
        return new SseAwareS3Client(createS3ClientBuilder().build());
    }

    protected abstract S3ClientBuilder createS3ClientBuilder();

    class SseAwareS3Client
            extends DelegatingS3Client
    {
        public SseAwareS3Client(S3Client delegate)
        {
            super(delegate);
        }

        @Override
        protected <T extends S3Request, ReturnT> ReturnT invokeOperation(T request, Function<T, ReturnT> operation)
        {
            if (request instanceof PutObjectRequest putObjectRequest) {
                PutObjectRequest.Builder putObjectRequestBuilder = putObjectRequest.toBuilder();
                putObjectRequestBuilder.sseCustomerAlgorithm(s3SseCustomerKey.algorithm());
                putObjectRequestBuilder.sseCustomerKey(s3SseCustomerKey.key());
                putObjectRequestBuilder.sseCustomerKeyMD5(s3SseCustomerKey.md5());
                return operation.apply((T) putObjectRequestBuilder.build());
            }
            else if (request instanceof GetObjectRequest getObjectRequest) {
                GetObjectRequest.Builder getObjectRequestBuilder = getObjectRequest.toBuilder();
                getObjectRequestBuilder.sseCustomerAlgorithm(s3SseCustomerKey.algorithm());
                getObjectRequestBuilder.sseCustomerKey(s3SseCustomerKey.key());
                getObjectRequestBuilder.sseCustomerKeyMD5(s3SseCustomerKey.md5());
                return operation.apply((T) getObjectRequestBuilder.build());
            }
            return operation.apply(request);
        }
    }
}
