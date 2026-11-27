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
package io.trino.plugin.inference;

import com.google.common.collect.ImmutableList;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.jetty.JettyHttpClient;
import io.trino.plugin.inference.OpenInferenceClient.InferenceRequest;
import io.trino.plugin.inference.OpenInferenceClient.InferenceResponse;
import io.trino.plugin.inference.OpenInferenceClient.Tensor;
import io.trino.plugin.inference.ptf.PredictDataProcessor;
import io.trino.plugin.inference.ptf.PredictTableFunction.PredictFunctionHandle;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.table.TableFunctionDataProcessor;
import io.trino.spi.function.table.TableFunctionProcessorState;
import io.trino.spi.function.table.TableFunctionProcessorState.Blocked;
import io.trino.spi.function.table.TableFunctionProcessorState.Processed;
import io.trino.spi.type.ArrayType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static io.trino.plugin.inference.InferenceErrorCode.INFERENCE_INVALID_RESPONSE;
import static io.trino.spi.function.table.TableFunctionProcessorState.Finished.FINISHED;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

/**
 * Tests the processor with input that the engine splits into several pages of one partition, which queries in tests do not produce reliably,
 * and with responses that the model servers in tests do not return.
 */
@TestInstance(PER_CLASS)
final class TestPredictDataProcessor
{
    private static final PredictFunctionHandle HOUSE_PRICE = new PredictFunctionHandle(
            "house-price",
            Optional.of("2"),
            ImmutableList.of(
                    new TensorSpecification("area", TensorDataType.FP64, ImmutableList.of(-1L, 1L)),
                    new TensorSpecification("rooms", TensorDataType.INT64, ImmutableList.of(-1L, 1L))),
            ImmutableList.of(new TensorSpecification("price", TensorDataType.FP64, ImmutableList.of(-1L, 1L))));

    // a model with an output that has any number of values per row
    private static final PredictFunctionHandle HOUSE_EMBEDDING = new PredictFunctionHandle(
            "house-embedding",
            Optional.empty(),
            HOUSE_PRICE.inputs(),
            ImmutableList.of(new TensorSpecification("embedding", TensorDataType.FP64, ImmutableList.of(-1L, -1L))));

    private final TestingOpenVinoModelServer server = new TestingOpenVinoModelServer();
    private final HttpClient httpClient = new JettyHttpClient();
    private final OpenInferenceClient client = new OpenInferenceClient(httpClient, new InferenceConfig().setEndpoint(server.getUri()));

    @AfterAll
    void tearDown()
    {
        httpClient.close();
        server.close();
    }

    @Test
    void testMultiplePagesInPartition()
            throws Exception
    {
        TableFunctionDataProcessor processor = new PredictDataProcessor(client, HOUSE_PRICE, 2);

        Page first = processPage(processor, page(new Double[] {1.0, 2.0, 3.0}, new Long[] {1L, 2L, 3L}));
        assertThat(doubles(first.getBlock(0))).containsExactly(6000.0, 12000.0, 18000.0);
        assertThat(longs(first.getBlock(1))).containsExactly(0L, 1L, 2L);

        // pass-through indexes continue from the rows of the previous page
        Page second = processPage(processor, page(new Double[] {4.0, null}, new Long[] {0L, 1L}));
        assertThat(doubles(second.getBlock(0))).containsExactly(4000.0, null);
        assertThat(longs(second.getBlock(1))).containsExactly(3L, 4L);

        assertThat(processor.process(null)).isSameAs(FINISHED);
    }

    @Test
    void testAllRowsWithNullInputs()
            throws Exception
    {
        long requests = server.getInferenceRequests("house-price");
        TableFunctionDataProcessor processor = new PredictDataProcessor(client, HOUSE_PRICE, 2);

        Page result = processPage(processor, page(new Double[] {null, 1.0}, new Long[] {1L, null}));
        assertThat(doubles(result.getBlock(0))).containsExactly(null, null);
        assertThat(longs(result.getBlock(1))).containsExactly(0L, 1L);
        assertThat(server.getInferenceRequests("house-price")).isEqualTo(requests);
    }

    @Test
    void testResponseShapeNotMatchingRequest()
            throws Exception
    {
        Page page = page(new Double[] {1.0, 2.0}, new Long[] {1L, 2L});
        List<Object> values = ImmutableList.of(1.0, 2.0, 3.0, 4.0, 5.0, 6.0);

        // the six values would fit two rows of three values, but the response has one row of six values for the two rows of the request
        assertTrinoExceptionThrownBy(() -> processPage(new PredictDataProcessor(respondingClient(embedding(ImmutableList.of(1L, 6L), values)), HOUSE_EMBEDDING, 2), page))
                .hasErrorCode(INFERENCE_INVALID_RESPONSE)
                .hasMessage("Model house-embedding returned the output embedding with the shape [1, 6] for 2 rows, but the first dimension of the shape must be the number of rows. The model must return a result for every row");
        assertTrinoExceptionThrownBy(() -> processPage(new PredictDataProcessor(respondingClient(embedding(ImmutableList.of(2L, 4L), values)), HOUSE_EMBEDDING, 2), page))
                .hasErrorCode(INFERENCE_INVALID_RESPONSE)
                .hasMessage("Model house-embedding returned 6 values for the output embedding, but the shape [2, 4] of the output describes a different number of values. The model server must respond in the format of the Open Inference Protocol");

        // the shape of the response determines the number of values in every row
        Page result = processPage(new PredictDataProcessor(respondingClient(embedding(ImmutableList.of(2L, 3L), values)), HOUSE_EMBEDDING, 2), page);
        assertThat(arrays(result.getBlock(0))).containsExactly(ImmutableList.of(1.0, 2.0, 3.0), ImmutableList.of(4.0, 5.0, 6.0));
    }

    /**
     * Returns a client that answers every inference request with the response.
     */
    private OpenInferenceClient respondingClient(InferenceResponse response)
    {
        return new OpenInferenceClient(httpClient, new InferenceConfig().setEndpoint(server.getUri()))
        {
            @Override
            public CompletableFuture<InferenceResponse> infer(String modelName, Optional<String> modelVersion, InferenceRequest inferenceRequest)
            {
                return completedFuture(response);
            }
        };
    }

    private static InferenceResponse embedding(List<Long> shape, List<Object> data)
    {
        return new InferenceResponse(ImmutableList.of(new Tensor("embedding", "FP64", shape, data)));
    }

    private static Page processPage(TableFunctionDataProcessor processor, Page page)
            throws Exception
    {
        List<Optional<Page>> input = ImmutableList.of(Optional.of(page));
        while (true) {
            TableFunctionProcessorState state = processor.process(input);
            if (state instanceof Blocked blocked) {
                blocked.getFuture().get();
                continue;
            }
            Processed processed = (Processed) state;
            assertThat(processed.isUsedInput()).isTrue();
            return processed.getResult();
        }
    }

    private static Page page(Double[] areas, Long[] rooms)
    {
        BlockBuilder areaBuilder = DOUBLE.createBlockBuilder(null, areas.length);
        for (Double area : areas) {
            if (area == null) {
                areaBuilder.appendNull();
            }
            else {
                DOUBLE.writeDouble(areaBuilder, area);
            }
        }
        BlockBuilder roomsBuilder = BIGINT.createBlockBuilder(null, rooms.length);
        for (Long room : rooms) {
            if (room == null) {
                roomsBuilder.appendNull();
            }
            else {
                BIGINT.writeLong(roomsBuilder, room);
            }
        }
        return new Page(areaBuilder.build(), roomsBuilder.build());
    }

    private static Double[] doubles(Block block)
    {
        Double[] values = new Double[block.getPositionCount()];
        for (int position = 0; position < block.getPositionCount(); position++) {
            if (!block.isNull(position)) {
                values[position] = DOUBLE.getDouble(block, position);
            }
        }
        return values;
    }

    private static Long[] longs(Block block)
    {
        Long[] values = new Long[block.getPositionCount()];
        for (int position = 0; position < block.getPositionCount(); position++) {
            values[position] = BIGINT.getLong(block, position);
        }
        return values;
    }

    private static List<List<Double>> arrays(Block block)
    {
        ArrayType type = new ArrayType(DOUBLE);
        ImmutableList.Builder<List<Double>> arrays = ImmutableList.builder();
        for (int position = 0; position < block.getPositionCount(); position++) {
            arrays.add(ImmutableList.copyOf(doubles(type.getObject(block, position))));
        }
        return arrays.build();
    }
}
