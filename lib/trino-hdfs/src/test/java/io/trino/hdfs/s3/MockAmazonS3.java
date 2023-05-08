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
package io.trino.hdfs.s3;

import com.amazonaws.SdkClientException;
import com.amazonaws.services.s3.AbstractAmazonS3;
import com.amazonaws.services.s3.model.AbortMultipartUploadRequest;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.CompleteMultipartUploadResult;
import com.amazonaws.services.s3.model.GetObjectMetadataRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.model.StorageClass;
import com.amazonaws.services.s3.model.UploadPartRequest;
import com.amazonaws.services.s3.model.UploadPartResult;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import static java.net.HttpURLConnection.HTTP_OK;
import static java.util.UUID.randomUUID;

public class MockAmazonS3
        extends AbstractAmazonS3
{
    private int getObjectHttpCode = HTTP_OK;
    private String getObjectS3ErrorCode;
    private int getObjectMetadataHttpCode = HTTP_OK;
    private GetObjectMetadataRequest getObjectMetadataRequest;
    private CannedAccessControlList acl;
    private boolean hasGlacierObjects;
    private boolean hasHadoopFolderMarkerObjects;
    private final List<UploadPartRequest> uploadParts = new ArrayList<>();

    public void setGetObjectHttpErrorCode(int getObjectHttpErrorCode)
    {
        this.getObjectHttpCode = getObjectHttpErrorCode;
    }

    public void setGetObjectS3ErrorCode(String getObjectS3ErrorCode)
    {
        this.getObjectS3ErrorCode = getObjectS3ErrorCode;
    }

    public void setGetObjectMetadataHttpCode(int getObjectMetadataHttpCode)
    {
        this.getObjectMetadataHttpCode = getObjectMetadataHttpCode;
    }

    public CannedAccessControlList getAcl()
    {
        return this.acl;
    }

    public void setHasGlacierObjects(boolean hasGlacierObjects)
    {
        this.hasGlacierObjects = hasGlacierObjects;
    }

    public void setHasHadoopFolderMarkerObjects(boolean hasHadoopFolderMarkerObjects)
    {
        this.hasHadoopFolderMarkerObjects = hasHadoopFolderMarkerObjects;
    }

    public GetObjectMetadataRequest getGetObjectMetadataRequest()
    {
        return getObjectMetadataRequest;
    }

    public List<UploadPartRequest> getUploadParts()
    {
        return uploadParts;
    }

    @Override
    public ObjectMetadata getObjectMetadata(GetObjectMetadataRequest getObjectMetadataRequest)
    {
        this.getObjectMetadataRequest = getObjectMetadataRequest;
        if (getObjectMetadataHttpCode != HTTP_OK) {
            AmazonS3Exception exception = new AmazonS3Exception("Failing getObjectMetadata call with " + getObjectMetadataHttpCode);
            exception.setStatusCode(getObjectMetadataHttpCode);
            throw exception;
        }
        return null;
    }

    @Override
    public S3Object getObject(GetObjectRequest getObjectRequest)
    {
        if (getObjectHttpCode != HTTP_OK) {
            AmazonS3Exception exception = new AmazonS3Exception("Failing getObject call with status code:" + getObjectHttpCode + "; error code:" + getObjectS3ErrorCode);
            exception.setStatusCode(getObjectHttpCode);
            exception.setErrorCode(getObjectS3ErrorCode);
            throw exception;
        }
        return null;
    }

    @Override
    public PutObjectResult putObject(PutObjectRequest putObjectRequest)
    {
        this.acl = putObjectRequest.getCannedAcl();
        return new PutObjectResult();
    }

    @Override
    public ListObjectsV2Result listObjectsV2(ListObjectsV2Request listObjectsV2Request)
    {
        final String continuationToken = "continue";

        ListObjectsV2Result listingV2 = new ListObjectsV2Result();

        if (continuationToken.equals(listObjectsV2Request.getContinuationToken())) {
            S3ObjectSummary standardTwo = new S3ObjectSummary();
            standardTwo.setStorageClass(StorageClass.Standard.toString());
            standardTwo.setKey("test/standardTwo");
            standardTwo.setLastModified(new Date());
            listingV2.getObjectSummaries().add(standardTwo);

            if (hasGlacierObjects) {
                S3ObjectSummary glacier = new S3ObjectSummary();
                glacier.setStorageClass(StorageClass.Glacier.toString());
                glacier.setKey("test/glacier");
                glacier.setLastModified(new Date());
                listingV2.getObjectSummaries().add(glacier);

                S3ObjectSummary deepArchive = new S3ObjectSummary();
                deepArchive.setStorageClass(StorageClass.DeepArchive.toString());
                deepArchive.setKey("test/deepArchive");
                deepArchive.setLastModified(new Date());
                listingV2.getObjectSummaries().add(deepArchive);
            }
        }
        else {
            S3ObjectSummary standardOne = new S3ObjectSummary();
            standardOne.setStorageClass(StorageClass.Standard.toString());
            standardOne.setKey("test/standardOne");
            standardOne.setLastModified(new Date());
            listingV2.getObjectSummaries().add(standardOne);
            listingV2.setTruncated(true);
            listingV2.setNextContinuationToken(continuationToken);

            if (hasHadoopFolderMarkerObjects) {
                S3ObjectSummary hadoopFolderMarker = new S3ObjectSummary();
                hadoopFolderMarker.setStorageClass(StorageClass.Standard.toString());
                hadoopFolderMarker.setKey("test/test_$folder$");
                hadoopFolderMarker.setLastModified(new Date());
                listingV2.getObjectSummaries().add(hadoopFolderMarker);
            }
        }

        return listingV2;
    }

    @Override
    public PutObjectResult putObject(String bucketName, String key, String content)
    {
        return new PutObjectResult();
    }

    @Override
    public InitiateMultipartUploadResult initiateMultipartUpload(InitiateMultipartUploadRequest request)
            throws SdkClientException
    {
        this.acl = request.getCannedACL();

        InitiateMultipartUploadResult result = new InitiateMultipartUploadResult();
        result.setUploadId(randomUUID().toString());
        return result;
    }

    @Override
    public UploadPartResult uploadPart(UploadPartRequest request)
            throws SdkClientException
    {
        uploadParts.add(request);
        return new UploadPartResult();
    }

    @Override
    public CompleteMultipartUploadResult completeMultipartUpload(CompleteMultipartUploadRequest request)
            throws SdkClientException
    {
        return new CompleteMultipartUploadResult();
    }

    @Override
    public void abortMultipartUpload(AbortMultipartUploadRequest request)
            throws SdkClientException
    {
    }

    @Override
    public void shutdown()
    {
    }
}
