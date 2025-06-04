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
package io.trino.server;

import com.google.inject.Inject;
import io.airlift.slice.OutputStreamSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import io.trino.FeaturesConfig;
import io.trino.FeaturesConfig.DataIntegrityVerification;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;

import static io.trino.execution.buffer.PagesSerdeUtil.NO_CHECKSUM;
import static io.trino.execution.buffer.PagesSerdeUtil.calculateChecksum;

public class PagesInputStreamFactory
{
    public static final int SERIALIZED_PAGES_MAGIC = 0xfea4f001;

    private final boolean dataIntegrityVerificationEnabled;

    @Inject
    public PagesInputStreamFactory(FeaturesConfig featuresConfig)
    {
        this.dataIntegrityVerificationEnabled = featuresConfig.getExchangeDataIntegrityVerification() != DataIntegrityVerification.NONE;
    }

    public void write(OutputStream stream, List<Slice> serializedPages)
            throws IOException
    {
        SliceOutput header = new OutputStreamSliceOutput(stream);
        header.writeInt(SERIALIZED_PAGES_MAGIC);
        header.writeLong(dataIntegrityVerificationEnabled ? calculateChecksum(serializedPages) : NO_CHECKSUM);
        header.writeInt(serializedPages.size());
        header.flush();

        for (Slice page : serializedPages) {
            page.getInput().transferTo(stream);
            stream.flush();
        }

        stream.close();
    }
}
