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
package io.trino.plugin.hive.s3select;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.GetObjectMetadataRequest;
import com.amazonaws.services.s3.model.InputSerialization;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.OutputSerialization;
import com.amazonaws.services.s3.model.ScanRange;
import com.amazonaws.services.s3.model.SelectObjectContentRequest;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoInput;
import io.trino.filesystem.TrinoInputFile;
import io.trino.filesystem.TrinoInputStream;

import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.time.Instant;
import java.util.Date;

import static com.amazonaws.services.s3.model.ExpressionType.SQL;
import static java.util.Objects.requireNonNull;

public class S3SelectInputFile
        implements TrinoInputFile
{
    private final AmazonS3 s3;
    private final TrinoS3SelectClient client;
    private final String query;
    private final Location location;
    private final long start;
    private final long length;
    private final InputSerialization inputSerialization;
    private final OutputSerialization outputSerialization;
    private final boolean enableScanRange;
    private Instant lastModified;

    public S3SelectInputFile(
            AmazonS3 s3,
            TrinoS3SelectClient client,
            String query,
            Location location,
            long start,
            long length,
            InputSerialization inputSerialization,
            OutputSerialization outputSerialization,
            boolean enableScanRange)
    {
        this.s3 = requireNonNull(s3, "s3 is null");
        this.client = requireNonNull(client, "client is null");
        this.query = requireNonNull(query, "query is null");
        this.location = requireNonNull(location, "location is null");
        this.start = start;
        this.length = length;
        this.inputSerialization = requireNonNull(inputSerialization, "inputSerialization is null");
        this.outputSerialization = requireNonNull(outputSerialization, "outputSerialization is null");
        this.enableScanRange = enableScanRange;
    }

    @Override
    public TrinoInput newInput()
    {
        return new S3SelectTrinoInput(location(), client, buildSelectObjectContentRequest());
    }

    @Override
    public TrinoInputStream newStream()
    {
        return new S3SelectInputStream(location(), client, buildSelectObjectContentRequest(), length);
    }

    @Override
    public long length()
            throws IOException
    {
        return length;
    }

    @Override
    public Instant lastModified()
            throws IOException
    {
        fetchLastModifiedTime();
        if (lastModified == null) {
            throw new NoSuchFileException("S3 file not found: " + location);
        }
        return lastModified;
    }

    @Override
    public boolean exists()
            throws IOException
    {
        fetchLastModifiedTime();
        return lastModified == null;
    }

    @Override
    public Location location()
    {
        return location;
    }

    private SelectObjectContentRequest buildSelectObjectContentRequest()
    {
        SelectObjectContentRequest selectObjectRequest = new SelectObjectContentRequest();
        selectObjectRequest.setBucketName(location.host().orElseThrow());
        selectObjectRequest.setKey(location.path());
        selectObjectRequest.setExpression(query);
        selectObjectRequest.setExpressionType(SQL);

        selectObjectRequest.setInputSerialization(inputSerialization);
        selectObjectRequest.setOutputSerialization(outputSerialization);

        if (enableScanRange) {
            ScanRange scanRange = new ScanRange();
            scanRange.setStart(start);
            scanRange.setEnd(start + length);
            selectObjectRequest.setScanRange(scanRange);
        }

        return selectObjectRequest;
    }

    private void fetchLastModifiedTime()
    {
        if (lastModified != null) {
            return;
        }

        ObjectMetadata metadata = s3.getObjectMetadata(new GetObjectMetadataRequest(location.host().orElseThrow(), location.path()));
        Date date = metadata.getLastModified();
        lastModified = date.toInstant();
    }
}
