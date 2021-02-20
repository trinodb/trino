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
package io.trino.plugin.pinot.encoders;

import org.apache.pinot.core.data.recordtransformer.RecordTransformer;
import org.apache.pinot.spi.data.readers.GenericRow;

public class ErrorSuppressingRecordTransformerWrapper
        implements RecordTransformer
{
    private final RecordTransformer originalTransformer;

    public ErrorSuppressingRecordTransformerWrapper(RecordTransformer originalTransformer)
    {
        this.originalTransformer = originalTransformer;
    }

    @Override
    public GenericRow transform(GenericRow record)
    {
        try {
            return originalTransformer.transform(record);
        }
        catch (Exception e) {
            //LOGGER.error("Suppressing error: " + e.getMessage(), e);
        }

        return null;
    }
}
