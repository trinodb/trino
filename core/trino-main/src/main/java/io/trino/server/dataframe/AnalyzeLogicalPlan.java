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
package io.trino.server.dataframe;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import com.google.inject.Provider;
import com.starburstdata.dataframe.analyzer.Analyzer;
import com.starburstdata.dataframe.analyzer.AnalyzerFactory;
import com.starburstdata.dataframe.analyzer.TrinoMetadata;
import com.starburstdata.dataframe.plan.LogicalPlan;
import com.starburstdata.dataframe.plan.TrinoPlan;
import io.airlift.compress.zstd.ZstdDecompressor;
import io.airlift.json.JsonCodec;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.FullConnectorSession;
import io.trino.Session;
import io.trino.metadata.InternalNode;
import io.trino.metadata.InternalNodeManager;
import io.trino.plugin.base.classloader.ClassLoaderSafeConnectorTableFunction;
import io.trino.spi.HostAddress;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.connector.ConnectorAccessControl;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.FixedSplitSource;
import io.trino.spi.function.table.AbstractConnectorTableFunction;
import io.trino.spi.function.table.Argument;
import io.trino.spi.function.table.ConnectorTableFunction;
import io.trino.spi.function.table.ConnectorTableFunctionHandle;
import io.trino.spi.function.table.Descriptor;
import io.trino.spi.function.table.ScalarArgument;
import io.trino.spi.function.table.ScalarArgumentSpecification;
import io.trino.spi.function.table.TableFunctionAnalysis;
import io.trino.spi.function.table.TableFunctionProcessorProvider;
import io.trino.spi.function.table.TableFunctionProcessorState;
import io.trino.spi.function.table.TableFunctionSplitProcessor;

import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.airlift.json.JsonCodec.jsonCodec;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.trino.metadata.GlobalFunctionCatalog.BUILTIN_SCHEMA;
import static io.trino.spi.function.table.ReturnTypeSpecification.GenericTable.GENERIC_TABLE;
import static io.trino.spi.function.table.TableFunctionProcessorState.Finished.FINISHED;
import static io.trino.spi.function.table.TableFunctionProcessorState.Processed.produced;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.Math.toIntExact;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

public class AnalyzeLogicalPlan
        implements Provider<ConnectorTableFunction>
{
    public static final String NAME = "analyze_logical_plan";
    private static final JsonCodec<LogicalPlan> LOGICAL_PLAN_CODEC = jsonCodec(LogicalPlan.class);
    private static final JsonCodec<TrinoPlan> TRINO_PLAN_CODEC = jsonCodec(TrinoPlan.class);
    private final TestingTrinoMetadataFactory testingTrinoMetadataFactory;
    private final AnalyzerFactory analyzerFactory;

    @Inject
    public AnalyzeLogicalPlan(TestingTrinoMetadataFactory testingTrinoMetadataFactory, AnalyzerFactory analyzerFactory)
    {
        this.testingTrinoMetadataFactory = requireNonNull(testingTrinoMetadataFactory, "testingTrinoMetadataFactory is null");
        this.analyzerFactory = requireNonNull(analyzerFactory, "analyzerFactory is null");
    }

    @Override
    public ConnectorTableFunction get()
    {
        return new ClassLoaderSafeConnectorTableFunction(new AnalyzeLogicalPlanFunction(testingTrinoMetadataFactory, analyzerFactory), getClass().getClassLoader());
    }

    public static class AnalyzeLogicalPlanFunction
            extends AbstractConnectorTableFunction
    {
        private static final String LOGICAL_PLAN_INPUT_NAME = "LOGICAL_PLAN";
        private static final String PREFIX = "$zstd:";
        private final TestingTrinoMetadataFactory testingTrinoMetadataFactory;
        private final AnalyzerFactory analyzerFactory;

        public AnalyzeLogicalPlanFunction(TestingTrinoMetadataFactory testingTrinoMetadataFactory, AnalyzerFactory analyzerFactory)
        {
            super(
                    BUILTIN_SCHEMA,
                    NAME,
                    List.of(ScalarArgumentSpecification.builder()
                            .name(LOGICAL_PLAN_INPUT_NAME)
                            .type(VARCHAR)
                            .build()),
                    GENERIC_TABLE);
            this.testingTrinoMetadataFactory = requireNonNull(testingTrinoMetadataFactory, "testingTrinoMetadataFactory is null");
            this.analyzerFactory = requireNonNull(analyzerFactory, "analyzerFactory is null");
        }

        @Override
        public TableFunctionAnalysis analyze(
                ConnectorSession connectorSession,
                ConnectorTransactionHandle transaction,
                Map<String, Argument> arguments,
                ConnectorAccessControl accessControl)
        {
            ScalarArgument argument = (ScalarArgument) getOnlyElement(arguments.values());
            String logicalPlan = ((Slice) argument.getValue()).toStringUtf8();
            if (logicalPlan.startsWith(PREFIX)) {
                String encoded = logicalPlan.substring(PREFIX.length());
                byte[] compressed = Base64.getDecoder().decode(encoded);
                byte[] logicalPlanBytes = new byte[toIntExact(ZstdDecompressor.getDecompressedSize(compressed, 0, compressed.length))];
                new ZstdDecompressor().decompress(compressed, 0, compressed.length, logicalPlanBytes, 0, logicalPlanBytes.length);
                logicalPlan = new String(logicalPlanBytes, UTF_8);
            }
            Session session = ((FullConnectorSession) connectorSession).getSession();
            TrinoMetadata trinoMetadata = testingTrinoMetadataFactory.create(session);
            Analyzer analyzer = analyzerFactory.create(trinoMetadata);
            String trinoPlan = TRINO_PLAN_CODEC.toJson(analyzer.resolve(LOGICAL_PLAN_CODEC.fromJson(logicalPlan)));
            return TableFunctionAnalysis.builder()
                    .handle(new AnalyzeLogicalPlanFunctionHandle(trinoPlan))
                    .returnedType(new Descriptor(ImmutableList.of(new Descriptor.Field("trino_plan", Optional.of(VARCHAR)))))
                    .build();
        }
    }

    public static ConnectorSplitSource getAnalyzeLogicalPlanFunctionSplitSource(InternalNodeManager internalNodeManager)
    {
        return new FixedSplitSource(new AnalyzeLogicalPlanFunctionSplit(internalNodeManager.getCoordinators().stream().map(InternalNode::getHostAndPort).collect(toImmutableList())));
    }

    public static class AnalyzeLogicalPlanFunctionSplit
            implements ConnectorSplit
    {
        private static final int INSTANCE_SIZE = instanceSize(AnalyzeLogicalPlanFunctionSplit.class);
        private final List<HostAddress> addresses;

        @JsonCreator
        public AnalyzeLogicalPlanFunctionSplit(@JsonProperty("addresses") List<HostAddress> addresses)
        {
            this.addresses = ImmutableList.copyOf(requireNonNull(addresses, "values is null"));
        }

        @Override
        public boolean isRemotelyAccessible()
        {
            return false;
        }

        @Override
        @JsonProperty
        public List<HostAddress> getAddresses()
        {
            return addresses;
        }

        @Override
        public Object getInfo()
        {
            return ImmutableMap.builder()
                    .put("addresses", addresses)
                    .buildOrThrow();
        }

        @Override
        public long getRetainedSizeInBytes()
        {
            return INSTANCE_SIZE;
        }
    }

    public static class AnalyzeLogicalPlanFunctionHandle
            implements ConnectorTableFunctionHandle
    {
        private final String trinoPlan;

        @JsonCreator
        public AnalyzeLogicalPlanFunctionHandle(@JsonProperty("trinoPlan") String trinoPlan)
        {
            this.trinoPlan = requireNonNull(trinoPlan, "trinoPlan is null");
        }

        @JsonProperty("trinoPlan")
        public String getTrinoPlan()
        {
            return trinoPlan;
        }
    }

    public static TableFunctionProcessorProvider getAnalyzeLogicalPlanFunctionProcessorProvider()
    {
        return new TableFunctionProcessorProvider()
        {
            @Override
            public TableFunctionSplitProcessor getSplitProcessor(ConnectorSession session, ConnectorTableFunctionHandle handle, ConnectorSplit split)
            {
                return new AnalyzeLogicalPlanFunctionProcessor(((AnalyzeLogicalPlanFunctionHandle) handle).getTrinoPlan());
            }
        };
    }

    public static class AnalyzeLogicalPlanFunctionProcessor
            implements TableFunctionSplitProcessor
    {
        private final PageBuilder pageBuilder = new PageBuilder(ImmutableList.of(VARCHAR));
        private final String trinoPlan;
        private boolean finished;

        public AnalyzeLogicalPlanFunctionProcessor(String trinoPlan)
        {
            this.trinoPlan = requireNonNull(trinoPlan, "trinoPlan is null");
        }

        @Override
        public TableFunctionProcessorState process()
        {
            checkState(pageBuilder.isEmpty(), "page builder not empty");

            if (finished) {
                return FINISHED;
            }

            BlockBuilder block = pageBuilder.getBlockBuilder(0);
            pageBuilder.declarePosition();
            VARCHAR.writeSlice(block, Slices.utf8Slice(trinoPlan));
            finished = true;
            Page page = pageBuilder.build();
            pageBuilder.reset();
            return produced(page);
        }
    }
}
