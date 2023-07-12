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

import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoInput;
import io.trino.filesystem.TrinoInputFile;
import io.trino.filesystem.TrinoInputStream;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ExpressionType;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.InputSerialization;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.OutputSerialization;
import software.amazon.awssdk.services.s3.model.RequestPayer;
import software.amazon.awssdk.services.s3.model.SelectObjectContentRequest;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.time.Instant;

import static java.util.Objects.requireNonNull;

public class S3SelectInputFile
        implements TrinoInputFile
{
    private final S3Client s3;
    private final S3AsyncClient asyncS3;
    private final String query;
    private final S3Location location;
    private final boolean enableScanRange;
    private final InputSerialization inputSerialization;
    private final OutputSerialization outputSerialization;
    private Long length;
    private Instant lastModified;
    private final RequestPayer requestPayer;


    public S3SelectInputFile(
            S3Client s3,
            S3AsyncClient asyncS3,
            S3Context context,
            S3Location location,
            String query,
            boolean enableScanRange,
            InputSerialization inputSerialization,
            OutputSerialization outputSerialization)
    {
        this.s3 = requireNonNull(s3, "s3 is null");
        this.asyncS3 = requireNonNull(asyncS3, "asyncS3 is null");
        this.query = requireNonNull(query, "query is null");
        this.location = requireNonNull(location, "location is null");
        location.location().verifyValidFileLocation();
        this.requestPayer = context.requestPayer();
        this.enableScanRange = enableScanRange;
        this.inputSerialization = requireNonNull(inputSerialization, "inputSerialization is null");
        this.outputSerialization = requireNonNull(outputSerialization, "outputSerialization is null");
    }

    @Override
    public TrinoInput newInput()
    {
        return new S3SelectInput(location(), asyncS3, buildSelectObjectContentRequest());
    }

    @Override
    public TrinoInputStream newStream()
    {
        return null; // new S3InputStream(location(), client, newGetObjectRequest(), length);
    }

    @Override
    public long length()
            throws IOException
    {
        if ((length == null) && !headObject()) {
            throw new FileNotFoundException(location.toString());
        }
        return length;
    }

    @Override
    public Instant lastModified()
            throws IOException
    {
        if ((lastModified == null) && !headObject()) {
            throw new FileNotFoundException(location.toString());
        }
        return lastModified;
    }

    @Override
    public boolean exists()
            throws IOException
    {
        return headObject();
    }

    @Override
    public Location location()
    {
        return location.location();
    }

    private SelectObjectContentRequest buildSelectObjectContentRequest()
    {
        SelectObjectContentRequest.Builder selectObjectContentRequest = SelectObjectContentRequest.builder()
                .bucket(location.bucket())
                .key(location.key())
                .expression(query)
                .expressionType(ExpressionType.SQL)
                .inputSerialization(inputSerialization)
                .outputSerialization(outputSerialization);

        if (enableScanRange) {
//            ScanRange scanRange = ScanRange.builder()
//                    .start(start)
//                    .end(start + length)
//                    .build();
//            selectObjectContentRequest.scanRange(scanRange);
        }

        return selectObjectContentRequest.build();
    }

    private boolean headObject()
            throws IOException
    {
        HeadObjectRequest request = HeadObjectRequest.builder()
                .requestPayer(requestPayer)
                .bucket(location.bucket())
                .key(location.key())
                .build();

        try {
            HeadObjectResponse response = s3.headObject(request);
            if (length == null) {
                length = response.contentLength();
            }
            if (lastModified == null) {
                lastModified = response.lastModified();
            }
            return true;
        }
        catch (NoSuchKeyException e) {
            return false;
        }
        catch (SdkException e) {
            throw new IOException("S3 HEAD request failed for file: " + location, e);
        }
    }
}
