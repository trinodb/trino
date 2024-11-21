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

import io.trino.filesystem.s3.S3Context.S3SseContext;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;

import static io.trino.filesystem.s3.S3FileSystemConfig.S3SseType.CUSTOMER;
import static software.amazon.awssdk.services.s3.model.ServerSideEncryption.AES256;
import static software.amazon.awssdk.services.s3.model.ServerSideEncryption.AWS_KMS;

public final class S3SseRequestConfigurator
{
    private S3SseRequestConfigurator() {}

    public static void setEncryptionSettings(PutObjectRequest.Builder builder, S3SseContext context)
    {
        switch (context.sseType()) {
            case NONE -> { /* ignored */ }
            case S3 -> builder.serverSideEncryption(AES256);
            case KMS -> context.sseKmsKeyId().ifPresent(builder.serverSideEncryption(AWS_KMS)::ssekmsKeyId);
            case CUSTOMER -> {
                context.sseCustomerKey().ifPresent(s3SseCustomerKey ->
                        builder.sseCustomerAlgorithm(s3SseCustomerKey.algorithm())
                                .sseCustomerKey(s3SseCustomerKey.key())
                                .sseCustomerKeyMD5(s3SseCustomerKey.md5()));
            }
        }
    }

    public static void setEncryptionSettings(CreateMultipartUploadRequest.Builder builder, S3SseContext context)
    {
        switch (context.sseType()) {
            case NONE -> { /* ignored */ }
            case S3 -> builder.serverSideEncryption(AES256);
            case KMS -> context.sseKmsKeyId().ifPresent(builder.serverSideEncryption(AWS_KMS)::ssekmsKeyId);
            case CUSTOMER -> {
                context.sseCustomerKey().ifPresent(s3SseCustomerKey ->
                        builder.sseCustomerAlgorithm(s3SseCustomerKey.algorithm())
                                .sseCustomerKey(s3SseCustomerKey.key())
                                .sseCustomerKeyMD5(s3SseCustomerKey.md5()));
            }
        }
    }

    public static void setEncryptionSettings(CompleteMultipartUploadRequest.Builder builder, S3SseContext context)
    {
        if (context.sseType() == CUSTOMER) {
            context.sseCustomerKey().ifPresent(s3SseCustomerKey ->
                    builder.sseCustomerAlgorithm(s3SseCustomerKey.algorithm())
                            .sseCustomerKey(s3SseCustomerKey.key())
                            .sseCustomerKeyMD5(s3SseCustomerKey.md5()));
        }
    }

    public static void setEncryptionSettings(GetObjectRequest.Builder builder, S3SseContext context)
    {
        if (context.sseType().equals(CUSTOMER)) {
            context.sseCustomerKey().ifPresent(s3SseCustomerKey ->
                    builder.sseCustomerAlgorithm(s3SseCustomerKey.algorithm())
                            .sseCustomerKey(s3SseCustomerKey.key())
                            .sseCustomerKeyMD5(s3SseCustomerKey.md5()));
        }
    }

    public static void setEncryptionSettings(HeadObjectRequest.Builder builder, S3SseContext context)
    {
        if (context.sseType().equals(CUSTOMER)) {
            context.sseCustomerKey().ifPresent(s3SseCustomerKey ->
                    builder.sseCustomerAlgorithm(s3SseCustomerKey.algorithm())
                            .sseCustomerKey(s3SseCustomerKey.key())
                            .sseCustomerKeyMD5(s3SseCustomerKey.md5()));
        }
    }

    public static void setEncryptionSettings(UploadPartRequest.Builder builder, S3SseContext context)
    {
        if (context.sseType() == CUSTOMER) {
            context.sseCustomerKey().ifPresent(s3SseCustomerKey ->
                    builder.sseCustomerAlgorithm(s3SseCustomerKey.algorithm())
                            .sseCustomerKey(s3SseCustomerKey.key())
                            .sseCustomerKeyMD5(s3SseCustomerKey.md5()));
        }
    }
}
