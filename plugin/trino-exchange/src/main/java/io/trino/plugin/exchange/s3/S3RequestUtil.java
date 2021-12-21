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
package io.trino.plugin.exchange.s3;

import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.ServerSideEncryption;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;
import software.amazon.awssdk.utils.Md5Utils;

import javax.crypto.SecretKey;

import java.util.Base64;
import java.util.Optional;
import java.util.function.Consumer;

public final class S3RequestUtil
{
    private S3RequestUtil()
    {
    }

    static void configureEncryption(Optional<SecretKey> secretKey, PutObjectRequest.Builder requestBuilder)
    {
        configureEncryption(secretKey, requestBuilder::sseCustomerAlgorithm, requestBuilder::sseCustomerKey, requestBuilder::sseCustomerKeyMD5);
    }

    static void configureEncryption(Optional<SecretKey> secretKey, CreateMultipartUploadRequest.Builder requestBuilder)
    {
        configureEncryption(secretKey, requestBuilder::sseCustomerAlgorithm, requestBuilder::sseCustomerKey, requestBuilder::sseCustomerKeyMD5);
    }

    static void configureEncryption(Optional<SecretKey> secretKey, UploadPartRequest.Builder requestBuilder)
    {
        configureEncryption(secretKey, requestBuilder::sseCustomerAlgorithm, requestBuilder::sseCustomerKey, requestBuilder::sseCustomerKeyMD5);
    }

    static void configureEncryption(Optional<SecretKey> secretKey, GetObjectRequest.Builder requestBuilder)
    {
        configureEncryption(secretKey, requestBuilder::sseCustomerAlgorithm, requestBuilder::sseCustomerKey, requestBuilder::sseCustomerKeyMD5);
    }

    static void configureEncryption(Optional<SecretKey> secretKey, HeadObjectRequest.Builder requestBuilder)
    {
        configureEncryption(secretKey, requestBuilder::sseCustomerAlgorithm, requestBuilder::sseCustomerKey, requestBuilder::sseCustomerKeyMD5);
    }

    static void configureEncryption(
            Optional<SecretKey> secretKey,
            Consumer<String> customAlgorithmSetter,
            Consumer<String> customKeySetter,
            Consumer<String> customMd5Setter)
    {
        secretKey.ifPresent(key -> {
            customAlgorithmSetter.accept(ServerSideEncryption.AES256.name());
            customKeySetter.accept(Base64.getEncoder().encodeToString(key.getEncoded()));
            customMd5Setter.accept(Md5Utils.md5AsBase64(key.getEncoded()));
        });
    }
}
