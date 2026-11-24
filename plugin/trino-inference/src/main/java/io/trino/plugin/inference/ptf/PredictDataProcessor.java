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
package io.trino.plugin.inference.ptf;

import com.google.common.collect.ImmutableList;
import io.trino.plugin.inference.OpenInferenceClient;
import io.trino.plugin.inference.OpenInferenceClient.InferenceRequest;
import io.trino.plugin.inference.OpenInferenceClient.InferenceResponse;
import io.trino.plugin.inference.OpenInferenceClient.RequestedOutput;
import io.trino.plugin.inference.OpenInferenceClient.Tensor;
import io.trino.plugin.inference.TensorSpecification;
import io.trino.plugin.inference.ptf.PredictTableFunction.PredictFunctionHandle;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.block.ArrayBlockBuilder;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.table.TableFunctionDataProcessor;
import io.trino.spi.function.table.TableFunctionProcessorState;
import io.trino.spi.type.ArrayType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.math.LongMath.saturatedMultiply;
import static io.trino.plugin.inference.InferenceErrorCode.INFERENCE_INVALID_RESPONSE;
import static io.trino.plugin.inference.OpenInferenceClient.getDone;
import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.spi.function.table.TableFunctionProcessorState.Blocked.blocked;
import static io.trino.spi.function.table.TableFunctionProcessorState.Finished.FINISHED;
import static io.trino.spi.function.table.TableFunctionProcessorState.Processed.usedInputAndProduced;
import static io.trino.spi.type.BigintType.BIGINT;
import static java.lang.Math.min;
import static java.util.Objects.requireNonNull;
import static java.util.Objects.requireNonNullElse;
import static java.util.stream.Collectors.joining;

/**
 * Scores one input page at a time. The rows of a page are split into batches of at most {@code maxBatchSize} rows,
 * and the batches are sent to the model server one after another. While a request is in flight, the processor
 * reports a blocked state, so it does not occupy a driver thread. Requests run concurrently across drivers and workers,
 * so the number of requests in flight is bounded by the number of drivers running the function.
 * <p>
 * A row with a null value in any model input, including a null array element, is not sent to the model server, and gets null outputs.
 */
public class PredictDataProcessor
        implements TableFunctionDataProcessor
{
    private final OpenInferenceClient client;
    private final PredictFunctionHandle handle;
    private final int maxBatchSize;
    private final List<RequestedOutput> requestedOutputs;

    // number of input rows processed so far, used to compute pass-through indexes
    private long processedPositions;

    // state of the page currently being scored: the positions of the rows sent to the model, the responses of the completed batches, and the request in flight
    private int[] positions;
    private List<InferenceResponse> responses;
    private CompletableFuture<InferenceResponse> pendingResponse;

    public PredictDataProcessor(OpenInferenceClient client, PredictFunctionHandle handle, int maxBatchSize)
    {
        this.client = requireNonNull(client, "client is null");
        this.handle = requireNonNull(handle, "handle is null");
        this.maxBatchSize = maxBatchSize;
        // some servers return only a default output, unless the request names the outputs
        this.requestedOutputs = handle.outputs().stream()
                .map(output -> new RequestedOutput(output.name()))
                .collect(toImmutableList());
    }

    @Override
    public TableFunctionProcessorState process(List<Optional<Page>> input)
    {
        if (input == null) {
            return FINISHED;
        }
        Page page = getOnlyElement(input).orElseThrow();
        if (positions == null) {
            positions = nonNullPositions(page);
            responses = new ArrayList<>();
        }

        if (pendingResponse != null) {
            if (!pendingResponse.isDone()) {
                return blockedUntilDone(pendingResponse);
            }
            responses.add(getDone(pendingResponse));
            pendingResponse = null;
        }

        int start = responses.size() * maxBatchSize;
        if (start < positions.length) {
            int end = min(start + maxBatchSize, positions.length);
            pendingResponse = client.infer(handle.modelName(), handle.modelVersion(), buildRequest(page, positions, start, end));
            return blockedUntilDone(pendingResponse);
        }

        Page result = buildResult(page.getPositionCount(), positions, responses);
        positions = null;
        responses = null;
        processedPositions += page.getPositionCount();
        return usedInputAndProduced(result);
    }

    private static TableFunctionProcessorState blockedUntilDone(CompletableFuture<?> future)
    {
        // complete normally also when the request fails, so the failure is reported by the next call to process
        return blocked(future.handle((_, _) -> null));
    }

    private int[] nonNullPositions(Page page)
    {
        int[] positions = new int[page.getPositionCount()];
        int count = 0;
        for (int position = 0; position < page.getPositionCount(); position++) {
            if (!hasNullInput(page, position)) {
                positions[count] = position;
                count++;
            }
        }
        return Arrays.copyOf(positions, count);
    }

    private boolean hasNullInput(Page page, int position)
    {
        for (int channel = 0; channel < page.getChannelCount(); channel++) {
            Block block = page.getBlock(channel);
            if (block.isNull(position)) {
                return true;
            }
            if (handle.inputs().get(channel).isArray()) {
                Block array = ((ArrayType) handle.inputs().get(channel).trinoType()).getObject(block, position);
                for (int index = 0; index < array.getPositionCount(); index++) {
                    if (array.isNull(index)) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    private InferenceRequest buildRequest(Page page, int[] positions, int start, int end)
    {
        ImmutableList.Builder<Tensor> tensors = ImmutableList.builder();
        for (int channel = 0; channel < handle.inputs().size(); channel++) {
            TensorSpecification input = handle.inputs().get(channel);
            Block block = page.getBlock(channel);
            List<Object> data = new ArrayList<>(end - start);
            long valuesPerRow = -1;
            for (int i = start; i < end; i++) {
                if (!input.isArray()) {
                    data.add(input.dataType().readValue(block, positions[i], input.name()));
                    continue;
                }
                Block array = ((ArrayType) input.trinoType()).getObject(block, positions[i]);
                if (valuesPerRow == -1) {
                    valuesPerRow = array.getPositionCount();
                }
                // all rows of a tensor have the same number of values
                if (input.valuesPerRow() != -1 && array.getPositionCount() != input.valuesPerRow()) {
                    throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "Model %s expects arrays with %s elements in column %s, but got an array with %s elements".formatted(
                            handle.modelName(),
                            input.valuesPerRow(),
                            input.name(),
                            array.getPositionCount()));
                }
                if (array.getPositionCount() != valuesPerRow) {
                    throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "Model %s expects arrays with the same number of elements in column %s for all rows that are sent in one request, but got arrays with %s and %s elements".formatted(
                            handle.modelName(),
                            input.name(),
                            valuesPerRow,
                            array.getPositionCount()));
                }
                for (int index = 0; index < array.getPositionCount(); index++) {
                    data.add(input.dataType().readValue(array, index, input.name()));
                }
            }
            tensors.add(new Tensor(input.name(), input.dataType().name(), shape(input, end - start, valuesPerRow), data));
        }
        return new InferenceRequest(tensors.build(), requestedOutputs);
    }

    private static List<Long> shape(TensorSpecification input, int rows, long valuesPerRow)
    {
        if (input.isArray()) {
            return ImmutableList.of((long) rows, valuesPerRow);
        }
        if (input.shape().size() == 2) {
            return ImmutableList.of((long) rows, 1L);
        }
        return ImmutableList.of((long) rows);
    }

    private Page buildResult(int positionCount, int[] positions, List<InferenceResponse> responses)
    {
        List<TensorSpecification> outputs = handle.outputs();
        Block[] blocks = new Block[outputs.size() + 1];
        for (int channel = 0; channel < outputs.size(); channel++) {
            TensorSpecification output = outputs.get(channel);
            List<List<Object>> rows = outputRows(output, positions.length, responses);
            String description = "output %s of model %s".formatted(output.name(), handle.modelName());
            BlockBuilder builder = output.trinoType().createBlockBuilder(null, positionCount);
            int next = 0;
            for (int position = 0; position < positionCount; position++) {
                if (next < positions.length && positions[next] == position) {
                    writeRow(output, builder, rows.get(next), description);
                    next++;
                }
                else {
                    builder.appendNull();
                }
            }
            blocks[channel] = builder.build();
        }

        // pass-through index of every output row, counted from the partition start
        BlockBuilder indexes = BIGINT.createFixedSizeBlockBuilder(positionCount);
        for (int position = 0; position < positionCount; position++) {
            BIGINT.writeLong(indexes, processedPositions + position);
        }
        blocks[outputs.size()] = indexes.build();

        return new Page(positionCount, blocks);
    }

    private static void writeRow(TensorSpecification output, BlockBuilder builder, List<Object> values, String description)
    {
        if (!output.isArray()) {
            output.dataType().writeValue(builder, values.getFirst(), description);
            return;
        }
        ((ArrayBlockBuilder) builder).buildEntry(elementBuilder -> {
            for (Object value : values) {
                output.dataType().writeValue(elementBuilder, value, description);
            }
        });
    }

    /**
     * Splits the values of an output into rows. Every response holds the rows of one batch, in the order of the requests.
     */
    private List<List<Object>> outputRows(TensorSpecification output, int rowCount, List<InferenceResponse> responses)
    {
        List<List<Object>> rows = new ArrayList<>(rowCount);
        for (int batch = 0; batch < responses.size(); batch++) {
            int batchRows = min(maxBatchSize, rowCount - batch * maxBatchSize);
            Tensor tensor = findOutput(output, responses.get(batch));
            List<Object> values = new ArrayList<>();
            flatten(tensor.data(), values);
            int valuesPerRow = valuesPerRow(output, tensor, batchRows, values.size());
            for (int row = 0; row < batchRows; row++) {
                rows.add(values.subList(row * valuesPerRow, (row + 1) * valuesPerRow));
            }
        }
        return rows;
    }

    /**
     * Returns the number of values in every row of an output in a response. The shape of the output must match the number of rows in the request,
     * the number of values in the response, and the shape in the model metadata, otherwise the values could be assigned to the wrong rows.
     */
    private int valuesPerRow(TensorSpecification output, Tensor tensor, int rows, int valueCount)
    {
        List<Long> shape = requireNonNullElse(tensor.shape(), ImmutableList.of());
        if (shape.isEmpty() || shape.getFirst() != rows) {
            throw new TrinoException(INFERENCE_INVALID_RESPONSE, "Model %s returned the output %s with the shape %s for %s, but the first dimension of the shape must be the number of rows. The model must return a result for every row".formatted(
                    handle.modelName(),
                    output.name(),
                    shape,
                    describeRows(rows)));
        }
        // the dimensions after the first one are the dimensions of the values of a row
        List<Long> rowShape = shape.subList(1, shape.size());
        long shapeValueCount = rows;
        for (long dimension : rowShape) {
            shapeValueCount = saturatedMultiply(shapeValueCount, dimension);
        }
        if (rowShape.stream().anyMatch(dimension -> dimension < 0) || shapeValueCount != valueCount) {
            throw new TrinoException(INFERENCE_INVALID_RESPONSE, "Model %s returned %s values for the output %s, but the shape %s of the output describes a different number of values. The model server must respond in the format of the Open Inference Protocol".formatted(
                    handle.modelName(),
                    valueCount,
                    output.name(),
                    shape));
        }
        int valuesPerRow = valueCount / rows;
        if (output.valuesPerRow() != -1 && valuesPerRow != output.valuesPerRow()) {
            String expected = "one value per row";
            if (output.isArray()) {
                expected = "%s values per row".formatted(output.valuesPerRow());
            }
            throw new TrinoException(INFERENCE_INVALID_RESPONSE, "Model %s returned the output %s with the shape %s for %s, but the shape %s of the output in the model metadata requires %s. Declare the shape of the output that the model returns in the configuration of the model on the model server".formatted(
                    handle.modelName(),
                    output.name(),
                    shape,
                    describeRows(rows),
                    output.shape(),
                    expected));
        }
        return valuesPerRow;
    }

    private static String describeRows(int rows)
    {
        if (rows == 1) {
            return "1 row";
        }
        return rows + " rows";
    }

    private Tensor findOutput(TensorSpecification output, InferenceResponse response)
    {
        for (Tensor tensor : response.outputs()) {
            if (output.name().equals(tensor.name())) {
                return tensor;
            }
        }
        // some servers ignore the requested outputs, and name the only output differently in the metadata and in the response
        if (handle.outputs().size() == 1 && response.outputs().size() == 1) {
            return response.outputs().getFirst();
        }
        String returnedOutputs = "no outputs";
        if (!response.outputs().isEmpty()) {
            returnedOutputs = "only the outputs " + response.outputs().stream().map(Tensor::name).collect(joining(", "));
        }
        throw new TrinoException(INFERENCE_INVALID_RESPONSE, "Response of model %s does not contain output %s, but %s. The model server must return every output that the metadata of the model declares, so declare only the outputs that the model returns in the configuration of the model on the model server".formatted(
                handle.modelName(),
                output.name(),
                returnedOutputs));
    }

    private static void flatten(List<?> data, List<Object> values)
    {
        if (data == null) {
            return;
        }
        for (Object value : data) {
            if (value instanceof List<?> nested) {
                flatten(nested, values);
            }
            else {
                values.add(value);
            }
        }
    }
}
