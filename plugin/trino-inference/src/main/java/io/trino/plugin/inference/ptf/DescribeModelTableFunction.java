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
import io.trino.plugin.inference.OpenInferenceClient;
import io.trino.plugin.inference.OpenInferenceClient.ModelMetadata;
import io.trino.plugin.inference.OpenInferenceClient.Tensor;
import io.trino.plugin.inference.TensorSpecification;
import io.trino.spi.connector.ConnectorAccessControl;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.function.table.AbstractConnectorTableFunction;
import io.trino.spi.function.table.Argument;
import io.trino.spi.function.table.ConnectorTableFunctionHandle;
import io.trino.spi.function.table.ReturnTypeSpecification.DescribedTable;
import io.trino.spi.function.table.ScalarArgumentSpecification;
import io.trino.spi.function.table.TableFunctionAnalysis;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.Type;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.airlift.slice.SizeOf.instanceSize;
import static io.trino.plugin.inference.ptf.PredictTableFunction.modelName;
import static io.trino.plugin.inference.ptf.PredictTableFunction.modelVersion;
import static io.trino.spi.function.table.Descriptor.descriptor;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;
import static java.util.Objects.requireNonNullElse;

/**
 * Returns the inputs and outputs of a model from its metadata, with the Trino type of the column that each tensor maps to.
 * The Trino type is null for tensors that the connector does not support.
 */
public class DescribeModelTableFunction
        extends AbstractConnectorTableFunction
{
    public static final String NAME = "describe_model";

    public static final List<String> COLUMN_NAMES = ImmutableList.of("kind", "name", "data_type", "shape", "trino_type");
    public static final List<Type> COLUMN_TYPES = ImmutableList.of(VARCHAR, VARCHAR, VARCHAR, new ArrayType(BIGINT), VARCHAR);

    private final OpenInferenceClient client;

    @Inject
    public DescribeModelTableFunction(OpenInferenceClient client)
    {
        super(PredictTableFunction.SCHEMA_NAME,
                NAME,
                ImmutableList.of(
                        ScalarArgumentSpecification.builder()
                                .name(PredictTableFunction.MODEL_NAME_ARGUMENT)
                                .type(VARCHAR)
                                .build(),
                        ScalarArgumentSpecification.builder()
                                .name(PredictTableFunction.MODEL_VERSION_ARGUMENT)
                                .type(VARCHAR)
                                .defaultValue(null)
                                .build()),
                new DescribedTable(descriptor(COLUMN_NAMES, COLUMN_TYPES)),
                "Describe the inputs and outputs of a model served over the Open Inference Protocol");
        this.client = requireNonNull(client, "client is null");
    }

    @Override
    public TableFunctionAnalysis analyze(
            ConnectorSession session,
            ConnectorTransactionHandle transaction,
            Map<String, Argument> arguments,
            ConnectorAccessControl accessControl)
    {
        ModelMetadata metadata = client.fetchModelMetadata(modelName(arguments), modelVersion(arguments));
        ImmutableList.Builder<ModelTensor> tensors = ImmutableList.builder();
        metadata.inputs().forEach(tensor -> tensors.add(ModelTensor.of("input", tensor)));
        metadata.outputs().forEach(tensor -> tensors.add(ModelTensor.of("output", tensor)));
        return TableFunctionAnalysis.builder()
                .handle(new DescribeModelFunctionHandle(tensors.build()))
                .build();
    }

    public record ModelTensor(String kind, String name, String dataType, List<Long> shape, Optional<String> trinoType)
    {
        public ModelTensor
        {
            requireNonNull(kind, "kind is null");
            requireNonNull(name, "name is null");
            requireNonNull(dataType, "dataType is null");
            shape = ImmutableList.copyOf(shape);
            requireNonNull(trinoType, "trinoType is null");
        }

        public static ModelTensor of(String kind, Tensor tensor)
        {
            return new ModelTensor(
                    kind,
                    tensor.name(),
                    tensor.datatype(),
                    requireNonNullElse(tensor.shape(), ImmutableList.of()),
                    TensorSpecification.fromMetadata(tensor).map(specification -> specification.trinoType().getDisplayName()));
        }
    }

    /**
     * The metadata is fetched during analysis, so the handle carries the result rows.
     */
    public record DescribeModelFunctionHandle(List<ModelTensor> tensors)
            implements ConnectorTableFunctionHandle
    {
        public DescribeModelFunctionHandle
        {
            tensors = ImmutableList.copyOf(tensors);
        }
    }

    /**
     * The single split of the function. All rows are in the function handle.
     */
    public record DescribeModelSplit()
            implements ConnectorSplit
    {
        private static final int INSTANCE_SIZE = instanceSize(DescribeModelSplit.class);

        @Override
        public long getRetainedSizeInBytes()
        {
            return INSTANCE_SIZE;
        }
    }
}
