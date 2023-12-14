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

    public static void addEncryptionSettings(PutObjectRequest.Builder builder, S3SseContext context)
    {
        switch (context.sseType()) {
            case NONE -> { /* ignored */ }
            case S3 -> builder.serverSideEncryption(AES256);
            case KMS -> builder.serverSideEncryption(AWS_KMS).ssekmsKeyId(context.sseKmsKeyId());
            case CUSTOMER -> {
                builder.sseCustomerAlgorithm(context.sseCustomerKey().algorithm());
                builder.sseCustomerKey(context.sseCustomerKey().key());
                builder.sseCustomerKeyMD5(context.sseCustomerKey().md5());
            }
        }
    }

    public static void addEncryptionSettings(CreateMultipartUploadRequest.Builder builder, S3SseContext context)
    {
        switch (context.sseType()) {
            case NONE -> { /* ignored */ }
            case S3 -> builder.serverSideEncryption(AES256);
            case KMS -> builder.serverSideEncryption(AWS_KMS).ssekmsKeyId(context.sseKmsKeyId());
            case CUSTOMER -> {
                builder.sseCustomerAlgorithm(context.sseCustomerKey().algorithm());
                builder.sseCustomerKey(context.sseCustomerKey().key());
                builder.sseCustomerKeyMD5(context.sseCustomerKey().md5());
            }
        }
    }

    public static void addEncryptionSettings(CompleteMultipartUploadRequest.Builder builder, S3SseContext context)
    {
        if (context.sseType() == CUSTOMER) {
            builder.sseCustomerAlgorithm(context.sseCustomerKey().algorithm());
            builder.sseCustomerKey(context.sseCustomerKey().key());
            builder.sseCustomerKeyMD5(context.sseCustomerKey().md5());
        }
    }

    public static void addEncryptionSettings(GetObjectRequest.Builder builder, S3SseContext context)
    {
        if (context.sseType().equals(CUSTOMER)) {
            builder.sseCustomerAlgorithm(context.sseCustomerKey().algorithm());
            builder.sseCustomerKey(context.sseCustomerKey().key());
            builder.sseCustomerKeyMD5(context.sseCustomerKey().md5());
        }
    }

    public static void addEncryptionSettings(HeadObjectRequest.Builder builder, S3SseContext context)
    {
        if (context.sseType().equals(CUSTOMER)) {
            builder.sseCustomerAlgorithm(context.sseCustomerKey().algorithm());
            builder.sseCustomerKey(context.sseCustomerKey().key());
            builder.sseCustomerKeyMD5(context.sseCustomerKey().md5());
        }
    }

    public static void addEncryptionSettings(UploadPartRequest.Builder builder, S3SseContext context)
    {
        if (context.sseType() == CUSTOMER) {
            builder.sseCustomerAlgorithm(context.sseCustomerKey().algorithm());
            builder.sseCustomerKey(context.sseCustomerKey().key());
            builder.sseCustomerKeyMD5(context.sseCustomerKey().md5());
        }
    }
}
