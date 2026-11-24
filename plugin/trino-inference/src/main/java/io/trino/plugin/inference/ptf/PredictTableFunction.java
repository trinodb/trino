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
import com.google.inject.Inject;
import io.airlift.slice.Slice;
import io.trino.plugin.inference.OpenInferenceClient;
import io.trino.plugin.inference.OpenInferenceClient.ModelMetadata;
import io.trino.plugin.inference.OpenInferenceClient.Tensor;
import io.trino.plugin.inference.TensorSpecification;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorAccessControl;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.function.table.AbstractConnectorTableFunction;
import io.trino.spi.function.table.Argument;
import io.trino.spi.function.table.ConnectorTableFunctionHandle;
import io.trino.spi.function.table.Descriptor;
import io.trino.spi.function.table.ScalarArgument;
import io.trino.spi.function.table.ScalarArgumentSpecification;
import io.trino.spi.function.table.TableArgument;
import io.trino.spi.function.table.TableArgumentSpecification;
import io.trino.spi.function.table.TableFunctionAnalysis;
import io.trino.spi.type.RowType;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.IntStream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.inference.InferenceErrorCode.INFERENCE_UNSUPPORTED_MODEL;
import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.spi.function.table.ReturnTypeSpecification.GenericTable.GENERIC_TABLE;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

/**
 * Sends the rows of the input table to a model served with the Open Inference Protocol (V2),
 * and returns the model outputs followed by all input columns.
 * <p>
 * Each model input is read from the input column with the same name. Each model output becomes an output column.
 * Tensors with one value per row map to columns of the tensor data type, and tensors with several values per row map to array columns.
 */
public class PredictTableFunction
        extends AbstractConnectorTableFunction
{
    public static final String SCHEMA_NAME = "system";
    public static final String NAME = "predict";

    static final String MODEL_NAME_ARGUMENT = "MODEL_NAME";
    static final String MODEL_VERSION_ARGUMENT = "MODEL_VERSION";

    private static final String INPUT_ARGUMENT = "INPUT";

    private static final String UNDECLARED_METADATA_HINT = "Declare the name, data type, and shape of every input and output in the configuration of the model on the model server";
    private static final String UNSUPPORTED_MODEL_HINT = "Change the model, or its configuration on the model server";
    // models can have an input for every column of a table, so error messages describe a limited number of columns
    private static final int MAX_DESCRIBED_COLUMNS = 20;

    private final OpenInferenceClient client;

    @Inject
    public PredictTableFunction(OpenInferenceClient client)
    {
        super(SCHEMA_NAME,
                NAME,
                ImmutableList.of(
                        TableArgumentSpecification.builder()
                                .name(INPUT_ARGUMENT)
                                .rowSemantics()
                                .passThroughColumns()
                                .build(),
                        ScalarArgumentSpecification.builder()
                                .name(MODEL_NAME_ARGUMENT)
                                .type(VARCHAR)
                                .build(),
                        ScalarArgumentSpecification.builder()
                                .name(MODEL_VERSION_ARGUMENT)
                                .type(VARCHAR)
                                .defaultValue(null)
                                .build()),
                GENERIC_TABLE,
                "Score the rows of the input table with a model served over the Open Inference Protocol");
        this.client = requireNonNull(client, "client is null");
    }

    @Override
    public TableFunctionAnalysis analyze(
            ConnectorSession session,
            ConnectorTransactionHandle transaction,
            Map<String, Argument> arguments,
            ConnectorAccessControl accessControl)
    {
        String modelName = modelName(arguments);
        Optional<String> modelVersion = modelVersion(arguments);

        ModelMetadata metadata = client.fetchModelMetadata(modelName, modelVersion);
        if (metadata.inputs().isEmpty()) {
            throw new TrinoException(INFERENCE_UNSUPPORTED_MODEL, "Metadata of model %s does not declare any inputs. %s".formatted(modelName, UNDECLARED_METADATA_HINT));
        }
        if (metadata.outputs().isEmpty()) {
            throw new TrinoException(INFERENCE_UNSUPPORTED_MODEL, "Metadata of model %s does not declare any outputs. %s".formatted(modelName, UNDECLARED_METADATA_HINT));
        }
        List<TensorSpecification> inputs = metadata.inputs().stream()
                .map(tensor -> toTensorSpecification("Input", modelName, tensor))
                .collect(toImmutableList());
        List<TensorSpecification> outputs = metadata.outputs().stream()
                .map(tensor -> toTensorSpecification("Output", modelName, tensor))
                .collect(toImmutableList());

        List<RowType.Field> inputFields = ((TableArgument) arguments.get(INPUT_ARGUMENT)).getRowType().getFields();
        ImmutableList.Builder<Integer> requiredColumns = ImmutableList.builder();
        for (TensorSpecification input : inputs) {
            int index = findColumn(inputFields, input, inputs, modelName);
            RowType.Field field = inputFields.get(index);
            if (!input.accepts(field.getType())) {
                throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "Column %s has type %s, but model %s expects %s (%s). Cast the column to the expected type".formatted(
                        input.name(),
                        field.getType(),
                        modelName,
                        input.trinoType(),
                        input.describe()));
            }
            requiredColumns.add(index);
        }

        Descriptor returnedType = new Descriptor(outputs.stream()
                .map(output -> new Descriptor.Field(output.name(), Optional.of(output.trinoType())))
                .collect(toImmutableList()));

        return TableFunctionAnalysis.builder()
                .requiredColumns(INPUT_ARGUMENT, requiredColumns.build())
                .returnedType(returnedType)
                .handle(new PredictFunctionHandle(modelName, modelVersion, inputs, outputs))
                .build();
    }

    /**
     * Column names in {@link TableArgument} are not canonical with respect to SQL identifier semantics, so they are matched case-insensitive.
     */
    private static int findColumn(List<RowType.Field> fields, TensorSpecification input, List<TensorSpecification> inputs, String modelName)
    {
        List<Integer> matches = IntStream.range(0, fields.size())
                .filter(index -> fields.get(index).getName().map(fieldName -> fieldName.equalsIgnoreCase(input.name())).orElse(false))
                .boxed()
                .collect(toImmutableList());
        if (matches.isEmpty()) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "Input table does not have a column for the input %s of model %s. The input table must have a column with the name of every input of the model: %s".formatted(
                    input.name(),
                    modelName,
                    describeColumns(inputs)));
        }
        if (matches.size() > 1) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "Input table has more than one column for the input %s of model %s, because column names are matched case-insensitively. Rename or remove columns in the input query".formatted(input.name(), modelName));
        }
        return matches.getFirst();
    }

    private static String describeColumns(List<TensorSpecification> inputs)
    {
        String columns = inputs.stream()
                .limit(MAX_DESCRIBED_COLUMNS)
                .map(input -> input.name() + " " + input.trinoType().getDisplayName())
                .collect(joining(", "));
        if (inputs.size() > MAX_DESCRIBED_COLUMNS) {
            columns += ", and %s more".formatted(inputs.size() - MAX_DESCRIBED_COLUMNS);
        }
        return columns;
    }

    static String modelName(Map<String, Argument> arguments)
    {
        return stringArgument(arguments, MODEL_NAME_ARGUMENT)
                .orElseThrow(() -> new TrinoException(INVALID_FUNCTION_ARGUMENT, "%s must not be null or empty".formatted(MODEL_NAME_ARGUMENT)));
    }

    static Optional<String> modelVersion(Map<String, Argument> arguments)
    {
        return stringArgument(arguments, MODEL_VERSION_ARGUMENT);
    }

    private static Optional<String> stringArgument(Map<String, Argument> arguments, String name)
    {
        Slice value = (Slice) ((ScalarArgument) arguments.get(name)).getValue();
        if (value == null || value.length() == 0) {
            return Optional.empty();
        }
        return Optional.of(value.toStringUtf8());
    }

    private static TensorSpecification toTensorSpecification(String kind, String modelName, Tensor tensor)
    {
        Optional<String> unsupportedReason = TensorSpecification.unsupportedReason(tensor);
        if (unsupportedReason.isPresent()) {
            throw new TrinoException(INFERENCE_UNSUPPORTED_MODEL, "%s %s of model %s %s. %s".formatted(kind, tensor.name(), modelName, unsupportedReason.get(), UNSUPPORTED_MODEL_HINT));
        }
        return TensorSpecification.fromMetadata(tensor).orElseThrow();
    }

    public record PredictFunctionHandle(
            String modelName,
            Optional<String> modelVersion,
            List<TensorSpecification> inputs,
            List<TensorSpecification> outputs)
            implements ConnectorTableFunctionHandle
    {
        public PredictFunctionHandle
        {
            requireNonNull(modelName, "modelName is null");
            requireNonNull(modelVersion, "modelVersion is null");
            inputs = ImmutableList.copyOf(inputs);
            outputs = ImmutableList.copyOf(outputs);
        }
    }
}
