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
package io.prestosql.elasticsearch.decoders;

import io.prestosql.spi.PrestoException;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.connector.ConnectorSession;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.search.SearchHit;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.function.Supplier;

import static io.prestosql.spi.StandardErrorCode.TYPE_MISMATCH;
import static io.prestosql.spi.type.TimestampType.TIMESTAMP;
import static java.time.format.DateTimeFormatter.ISO_DATE_TIME;

public class TimestampDecoder
        implements Decoder
{
    private final String path;
    private final ZoneId zoneId;

    public TimestampDecoder(ConnectorSession session, String path)
    {
        this.path = path;
        this.zoneId = ZoneId.of(session.getTimeZoneKey().getId());
    }

    @Override
    public void decode(SearchHit hit, Supplier<Object> getter, BlockBuilder output)
    {
        DocumentField documentField = hit.getFields().get(path);
        if (documentField == null) {
            output.appendNull();
        }
        else if (documentField.getValues().size() > 1) {
            throw new PrestoException(TYPE_MISMATCH, "Expected single value for column: " + path);
        }
        else {
            TIMESTAMP.writeLong(output,
                    ISO_DATE_TIME.parse(documentField.getValue(), LocalDateTime::from)
                            .atZone(zoneId)
                            .toInstant()
                            .toEpochMilli());
        }
    }
}
