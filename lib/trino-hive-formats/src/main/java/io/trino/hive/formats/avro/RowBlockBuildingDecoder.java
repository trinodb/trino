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
package io.trino.hive.formats.avro;

import io.trino.hive.formats.avro.model.AvroReadAction;
import io.trino.hive.formats.avro.model.DefaultValueFieldRecordFieldReadAction;
import io.trino.hive.formats.avro.model.ReadFieldAction;
import io.trino.hive.formats.avro.model.RecordFieldReadAction;
import io.trino.hive.formats.avro.model.RecordReadAction;
import io.trino.hive.formats.avro.model.SkipFieldRecordFieldReadAction;
import io.trino.hive.formats.avro.model.SkipFieldRecordFieldReadAction.SkipAction;
import io.trino.spi.PageBuilder;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.RowBlockBuilder;
import org.apache.avro.Resolver;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;

import java.io.IOException;
import java.util.function.IntFunction;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class RowBlockBuildingDecoder
        implements BlockBuildingDecoder
{
    private final RowBuildingAction[] buildSteps;

    public RowBlockBuildingDecoder(Schema writeSchema, Schema readSchema, AvroTypeBlockHandler typeManager)
            throws AvroTypeException
    {
        this(AvroReadAction.fromAction(Resolver.resolve(writeSchema, readSchema, new GenericData())), typeManager);
    }

    public RowBlockBuildingDecoder(AvroReadAction action, AvroTypeBlockHandler typeManager)
            throws AvroTypeException

    {
        if (!(action instanceof RecordReadAction recordReadAction)) {
            throw new AvroTypeException("Write and Read Schemas must be records when building a row block building decoder. Illegal action: " + action);
        }
        buildSteps = new RowBuildingAction[recordReadAction.fieldReadActions().size()];
        int i = 0;
        for (RecordFieldReadAction fieldAction : recordReadAction.fieldReadActions()) {
            buildSteps[i] = switch (fieldAction) {
                case DefaultValueFieldRecordFieldReadAction defaultValueFieldRecordFieldReadAction -> new ConstantBlockAction(
                        getDefaultBlockBuilder(defaultValueFieldRecordFieldReadAction.fieldSchema(),
                                defaultValueFieldRecordFieldReadAction.defaultBytes(), typeManager),
                        defaultValueFieldRecordFieldReadAction.outputChannel());
                case ReadFieldAction readFieldAction -> new BuildIntoBlockAction(typeManager.blockBuildingDecoderFor(readFieldAction.readAction()),
                        readFieldAction.outputChannel());
                case SkipFieldRecordFieldReadAction skipFieldRecordFieldReadAction -> new SkipSchemaBuildingAction(skipFieldRecordFieldReadAction.skipAction());
            };
            i++;
        }
    }

    @Override
    public void decodeIntoBlock(Decoder decoder, BlockBuilder builder)
            throws IOException
    {
        ((RowBlockBuilder) builder).buildEntry(fieldBuilders -> decodeIntoBlockProvided(decoder, fieldBuilders::get));
    }

    protected void decodeIntoPageBuilder(Decoder decoder, PageBuilder builder)
            throws IOException
    {
        builder.declarePosition();
        decodeIntoBlockProvided(decoder, builder::getBlockBuilder);
    }

    protected void decodeIntoBlockProvided(Decoder decoder, IntFunction<BlockBuilder> fieldBlockBuilder)
            throws IOException
    {
        for (RowBuildingAction buildStep : buildSteps) {
            switch (buildStep) {
                case SkipSchemaBuildingAction skipSchemaBuildingAction -> skipSchemaBuildingAction.skip(decoder);
                case BuildIntoBlockAction buildIntoBlockAction -> buildIntoBlockAction.decode(decoder, fieldBlockBuilder);
                case ConstantBlockAction constantBlockAction -> constantBlockAction.addConstant(fieldBlockBuilder);
            }
        }
    }

    sealed interface RowBuildingAction
            permits BuildIntoBlockAction, ConstantBlockAction, SkipSchemaBuildingAction
    {}

    private static final class BuildIntoBlockAction
            implements RowBuildingAction
    {
        private final BlockBuildingDecoder delegate;
        private final int outputChannel;

        public BuildIntoBlockAction(BlockBuildingDecoder delegate, int outputChannel)
        {
            this.delegate = requireNonNull(delegate, "delegate is null");
            checkArgument(outputChannel >= 0, "outputChannel must be positive");
            this.outputChannel = outputChannel;
        }

        public void decode(Decoder decoder, IntFunction<BlockBuilder> channelSelector)
                throws IOException
        {
            delegate.decodeIntoBlock(decoder, channelSelector.apply(outputChannel));
        }
    }

    protected static final class ConstantBlockAction
            implements RowBuildingAction
    {
        private final AvroReadAction.IoConsumer<BlockBuilder> addConstantFunction;
        private final int outputChannel;

        public ConstantBlockAction(AvroReadAction.IoConsumer<BlockBuilder> addConstantFunction, int outputChannel)
        {
            this.addConstantFunction = requireNonNull(addConstantFunction, "addConstantFunction is null");
            checkArgument(outputChannel >= 0, "outputChannel must be positive");
            this.outputChannel = outputChannel;
        }

        public void addConstant(IntFunction<BlockBuilder> channelSelector)
                throws IOException
        {
            addConstantFunction.accept(channelSelector.apply(outputChannel));
        }
    }

    public static final class SkipSchemaBuildingAction
            implements RowBuildingAction
    {
        private final SkipAction skipAction;

        SkipSchemaBuildingAction(SkipAction skipAction)
        {
            this.skipAction = requireNonNull(skipAction, "skipAction is null");
        }

        public void skip(Decoder decoder)
                throws IOException
        {
            skipAction.skip(decoder);
        }
    }

    // Avro supports default values for reader record fields that are missing in the writer schema
    // the bytes representing the default field value are passed to a block building decoder
    // so that it can pack the block appropriately for the default type.
    private static AvroReadAction.IoConsumer<BlockBuilder> getDefaultBlockBuilder(Schema fieldSchema, byte[] defaultBytes, AvroTypeBlockHandler typeManager)
            throws AvroTypeException
    {
        BlockBuildingDecoder buildingDecoder = typeManager.blockBuildingDecoderFor(AvroReadAction.fromAction(Resolver.resolve(fieldSchema, fieldSchema)));
        BinaryDecoder reuse = DecoderFactory.get().binaryDecoder(defaultBytes, null);
        return blockBuilder -> buildingDecoder.decodeIntoBlock(DecoderFactory.get().binaryDecoder(defaultBytes, reuse), blockBuilder);
    }
}
