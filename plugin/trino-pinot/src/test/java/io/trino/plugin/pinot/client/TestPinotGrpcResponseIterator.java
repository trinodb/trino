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
package io.trino.plugin.pinot.client;

import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import io.trino.plugin.pinot.PinotException;
import io.trino.plugin.pinot.client.PinotGrpcDataFetcher.PinotGrpcServerQueryClient.ResponseIterator;
import org.apache.pinot.common.datatable.DataTable;
import org.apache.pinot.common.proto.Server;
import org.apache.pinot.core.common.datatable.DataTableBuilderFactory;
import org.apache.pinot.spi.utils.CommonConstants.Query.Response.MetadataKeys;
import org.apache.pinot.spi.utils.CommonConstants.Query.Response.ResponseType;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Iterator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestPinotGrpcResponseIterator
{
    private static final String QUERY = "SELECT count(*) FROM orders";
    private static final int SERVER_SEGMENT_MISSING = 235;

    @Test
    void testMetadataWithoutExceptionsEndsTheStream()
            throws IOException
    {
        ResponseIterator iterator = new ResponseIterator(responses(metadataResponse(DataTableBuilderFactory.getEmptyDataTable())), QUERY);

        assertThat(iterator.hasNext()).isFalse();
    }

    @Test
    void testMetadataWithExceptionIsReported()
            throws IOException
    {
        DataTable metadata = DataTableBuilderFactory.getEmptyDataTable();
        metadata.addException(SERVER_SEGMENT_MISSING, "segment orders_OFFLINE_0 is missing");
        ResponseIterator iterator = new ResponseIterator(responses(metadataResponse(metadata)), QUERY);

        assertThatThrownBy(iterator::hasNext)
                .isInstanceOf(PinotException.class)
                .hasMessageContaining("Encountered 1 exceptions")
                .hasMessageContaining("Error code: 235")
                .hasMessageContaining("segment orders_OFFLINE_0 is missing");
    }

    @Test
    void testEmptyMetadataPayloadEndsTheStream()
    {
        Server.ServerResponse response = Server.ServerResponse.newBuilder()
                .putMetadata(MetadataKeys.RESPONSE_TYPE, ResponseType.METADATA)
                .build();
        ResponseIterator iterator = new ResponseIterator(responses(response), QUERY);

        assertThat(iterator.hasNext()).isFalse();
    }

    private static Iterator<Server.ServerResponse> responses(Server.ServerResponse... responses)
    {
        return ImmutableList.copyOf(responses).iterator();
    }

    private static Server.ServerResponse metadataResponse(DataTable dataTable)
            throws IOException
    {
        return Server.ServerResponse.newBuilder()
                .putMetadata(MetadataKeys.RESPONSE_TYPE, ResponseType.METADATA)
                .setPayload(ByteString.copyFrom(dataTable.toBytes()))
                .build();
    }
}
