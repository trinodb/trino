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
package io.trino.operator.aggregation;

import com.google.common.collect.ImmutableList;
import io.trino.operator.GroupByIdBlock;
import io.trino.spi.block.Block;
import io.trino.spi.block.DictionaryBlock;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.function.AccumulatorState;
import io.trino.spi.function.GroupedAccumulatorState;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.util.ArrayDeque;
import java.util.List;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.Iterables.cycle;
import static com.google.common.collect.Iterables.limit;
import static java.lang.invoke.MethodHandles.collectArguments;
import static java.lang.invoke.MethodHandles.dropArguments;
import static java.lang.invoke.MethodHandles.explicitCastArguments;
import static java.lang.invoke.MethodHandles.guardWithTest;
import static java.lang.invoke.MethodHandles.lookup;
import static java.lang.invoke.MethodHandles.permuteArguments;
import static java.lang.invoke.MethodHandles.publicLookup;

final class AggregationLoopBuilder
{
    private static final MethodHandle GET_GROUP_ID;
    private static final MethodHandle SET_GROUP_ID;

    private static final MethodHandle MASK_IS_SELECT_ALL;
    private static final MethodHandle MASK_GET_POSITION_COUNT;
    private static final MethodHandle MASK_GET_SELECTED_POSITION_COUNT;
    private static final MethodHandle MASK_GET_SELECTED_POSITIONS;

    private static final MethodHandle RLE_GET_VALUE;

    private static final MethodHandle DICTIONARY_GET_DICTIONARY;
    private static final MethodHandle DICTIONARY_GET_RAW_IDS;
    private static final MethodHandle DICTIONARY_GET_RAW_IDS_OFFSET;

    private static final MethodHandle IS_INSTANCE;
    private static final MethodHandle INT_ADD_FUNCTION;
    private static final MethodHandle INT_INCREMENT;
    private static final MethodHandle INT_GREATER_THAN;

    static {
        try {
            GET_GROUP_ID = publicLookup().findVirtual(GroupByIdBlock.class, "getGroupId", MethodType.methodType(long.class, int.class));
            SET_GROUP_ID = publicLookup().findVirtual(GroupedAccumulatorState.class, "setGroupId", MethodType.methodType(void.class, long.class));

            MASK_GET_POSITION_COUNT = publicLookup().findVirtual(AggregationMask.class, "getPositionCount", MethodType.methodType(int.class));
            MASK_IS_SELECT_ALL = publicLookup().findVirtual(AggregationMask.class, "isSelectAll", MethodType.methodType(boolean.class));
            MASK_GET_SELECTED_POSITION_COUNT = publicLookup().findVirtual(AggregationMask.class, "getSelectedPositionCount", MethodType.methodType(int.class));
            MASK_GET_SELECTED_POSITIONS = publicLookup().findVirtual(AggregationMask.class, "getSelectedPositions", MethodType.methodType(int[].class));

            RLE_GET_VALUE = lookup().findVirtual(RunLengthEncodedBlock.class, "getValue", MethodType.methodType(Block.class));

            DICTIONARY_GET_DICTIONARY = lookup().findVirtual(DictionaryBlock.class, "getDictionary", MethodType.methodType(Block.class));
            DICTIONARY_GET_RAW_IDS = lookup().findVirtual(DictionaryBlock.class, "getRawIds", MethodType.methodType(int[].class));
            DICTIONARY_GET_RAW_IDS_OFFSET = lookup().findVirtual(DictionaryBlock.class, "getRawIdsOffset", MethodType.methodType(int.class));

            IS_INSTANCE = publicLookup().findVirtual(Class.class, "isInstance", MethodType.methodType(boolean.class, Object.class));
            INT_ADD_FUNCTION = lookup().findStatic(AggregationLoopBuilder.class, "add", MethodType.methodType(int.class, int.class, int.class));
            INT_INCREMENT = lookup().findStatic(AggregationLoopBuilder.class, "increment", MethodType.methodType(int.class, int.class));
            INT_GREATER_THAN = lookup().findStatic(AggregationLoopBuilder.class, "greaterThan", MethodType.methodType(boolean.class, int.class, int.class));
        }
        catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    private AggregationLoopBuilder() {}

    /**
     * Converts ungrouped input function into an input function that has an additional parameter for the GroupIdBlock that
     * is used to set the group id on each aggregation state.
     */
    public static MethodHandle toGroupedInputFunction(MethodHandle inputFunction, int stateCount)
    {
        // input function will be:
        // state0, state1, ..., stateN, block0, position0, block1, position1, ..., blockN, positionN

        // convert unused boolean parameter into a call to set the groupId on the groupedState
        // state0, groupedState0, groupId, state1, groupedState1, groupId, ..., stateN, groupedStateN, groupId, block0, position0, block1, position1, ..., blockN, positionN
        for (int i = 0; i < stateCount; i++) {
            inputFunction = collectArguments(inputFunction, 1 + (i * 3), SET_GROUP_ID);
        }

        // cast duped state arguments
        // state0, state0, groupId, state1, state1, groupId, ..., stateN, stateN, groupId, block0, position0, block1, position1, ..., blockN, positionN
        MethodType type = inputFunction.type();
        for (int i = 0; i < stateCount; i++) {
            type = type.changeParameterType(1 + (i * 3), AccumulatorState.class);
        }
        inputFunction = explicitCastArguments(inputFunction, type);

        // deduplicate groupId and state arguments
        // state0, state1, ..., stateN, groupId, block0, position0, block1, position1, ..., blockN, positionN
        int[] reorder = new int[inputFunction.type().parameterCount()];
        for (int stateIndex = 0; stateIndex < stateCount; stateIndex++) {
            reorder[(stateIndex * 3)] = stateIndex;
            reorder[(stateIndex * 3) + 1] = stateIndex;
            reorder[(stateIndex * 3) + 2] = stateCount;
        }
        int nonStateParameters = inputFunction.type().parameterCount() - (stateCount * 3);
        for (int i = 0; i < nonStateParameters; i++) {
            reorder[(stateCount * 3) + i] = stateCount + 1 + i;
        }
        MethodType newType = inputFunction.type();
        for (int i = 0; i < stateCount; i++) {
            newType = newType.dropParameterTypes(i + 1, i + 3);
        }
        newType = newType.insertParameterTypes(stateCount, long.class);
        inputFunction = permuteArguments(inputFunction, newType, reorder);

        // get groupId from GroupIdBlock
        // state0, state1, ..., stateN, groupIdBlock, groupIdPosition, block0, position0, block1, position1, ..., blockN, positionN
        inputFunction = collectArguments(inputFunction, stateCount, GET_GROUP_ID);

        // cast groupId block
        // state0, state1, ..., stateN, groupIdBlock, groupIdPosition, block0, position0, block1, position1, ..., blockN, positionN
        inputFunction = explicitCastArguments(inputFunction, inputFunction.type().changeParameterType(stateCount, Block.class));

        return inputFunction;
    }

    /**
     * Build a loop over the aggregation function.  Internally, there are multiple loops generated that are specialized for
     * RLE, Dictionary, and basic blocks, and for masked or unmasked input.  The method handle is expected to have a {@link Block} and int
     * position argument for each parameter.  The returned method handle signature, will start with as @link {@link AggregationMask}
     * and then a single {@link Block} for each parameter.
     */
    public static MethodHandle buildLoop(MethodHandle function, int stateCount, int parameterCount, int lambdaParameterCount)
    {
        // verify signature
        List<Class<?>> expectedParameterTypes = ImmutableList.<Class<?>>builder()
                .addAll(function.type().parameterList().subList(0, stateCount))
                .addAll(limit(cycle(Block.class, int.class), parameterCount * 2))
                .addAll(function.type().parameterList().subList(stateCount + (parameterCount * 2), function.type().parameterCount()))
                .build();
        MethodType expectedSignature = MethodType.methodType(void.class, expectedParameterTypes);
        checkArgument(function.type().equals(expectedSignature), "Expected function signature to be %s, but is %s", expectedSignature, function.type());

        // add dummy loop position to the front of the loop
        function = dropArguments(function, 0, int.class);

        function = buildParameterBlockTypeSelector(function, stateCount, lambdaParameterCount, new ArrayDeque<>(parameterCount), parameterCount);
        return function;
    }

    /**
     * Builds a method handle that switches based on the parameter block type, and then delegates to a loop for the specific block type.
     * This structure is build recursively in a bottoms up style.
     */
    private static MethodHandle buildParameterBlockTypeSelector(MethodHandle function, int stateCount, int lambdaParameterCount, ArrayDeque<BlockType> currentTypes, int remainingParameters)
    {
        // If there are no more parameters, build the core loop for the selected block types
        if (remainingParameters == 0) {
            return buildCoreLoop(function, stateCount, ImmutableList.copyOf(currentTypes));
        }

        // otherwise, recurse and build a block type specific loop for each block type

        // RLE
        currentTypes.addLast(BlockType.RLE);
        MethodHandle rleMethodHandle = buildParameterBlockTypeSelector(function, stateCount, lambdaParameterCount, currentTypes, remainingParameters - 1);
        rleMethodHandle = preprocessRleParameter(
                rleMethodHandle,
                rleMethodHandle.type().parameterCount() - lambdaParameterCount - remainingParameters);
        currentTypes.removeLast();

        // Dictionary
        currentTypes.addLast(BlockType.DICTIONARY);
        MethodHandle dictionaryMethodHandle = buildParameterBlockTypeSelector(function, stateCount, lambdaParameterCount, currentTypes, remainingParameters - 1);
        dictionaryMethodHandle = preprocessDictionaryParameter(
                dictionaryMethodHandle,
                dictionaryMethodHandle.type().parameterCount() - lambdaParameterCount - remainingParameters - 2);
        currentTypes.removeLast();

        // Basic
        currentTypes.addLast(BlockType.BASIC);
        MethodHandle basicMethodHandle = buildParameterBlockTypeSelector(function, stateCount, lambdaParameterCount, currentTypes, remainingParameters - 1);
        currentTypes.removeLast();

        // combine the block type specific loops into a single method handle that selects the correct loop based on block type.
        return guardWithTest(
                blockParameterIndexOf(
                        basicMethodHandle.type(),
                        basicMethodHandle.type().parameterCount() - lambdaParameterCount - remainingParameters,
                        RunLengthEncodedBlock.class),
                rleMethodHandle,
                guardWithTest(
                        blockParameterIndexOf(
                                basicMethodHandle.type(),
                                basicMethodHandle.type().parameterCount() - lambdaParameterCount - remainingParameters,
                                DictionaryBlock.class),
                        dictionaryMethodHandle,
                        basicMethodHandle));
    }

    /**
     * Builds the core loop.  Parameters of input function are processed for a specific block type.  Then
     * the function is wrapped into two separate loops: one that process all positions, and one that processes
     * only the selected positions in the mask.  Finally, these loops are wrapped in an if statement to
     * select the correct loop based on the @see {@link AggregationMask} state.
     */
    private static MethodHandle buildCoreLoop(MethodHandle function, int stateCount, List<BlockType> blockTypes)
    {
        // for each parameter, process for the specified block type
        for (int parameterIndex = blockTypes.size() - 1; parameterIndex >= 0; parameterIndex--) {
            int parameterStart = (parameterIndex * 2) + 1 + stateCount;
            function = switch (blockTypes.get(parameterIndex)) {
                case RLE -> processRleParameter(parameterStart, function);
                case DICTIONARY -> processDictionaryParameter(parameterStart, function);
                case BASIC -> processBasicParameter(parameterStart, function);
            };
        }

        MethodHandle selectAllLoop = selectAllLoop(function);
        MethodHandle maskedLoop = maskedLoop(function);

        return guardWithTest(MASK_IS_SELECT_ALL, selectAllLoop, maskedLoop);
    }

    /**
     * Process the function on every position.
     */
    private static MethodHandle selectAllLoop(MethodHandle function)
    {
        // add unused iterationCount argument in the second position, which required by countedLoop.
        function = dropArguments(function, 1, AggregationMask.class);

        return MethodHandles.countedLoop(MASK_GET_POSITION_COUNT, null, function);
    }

    /**
     * Process the function on only the selected position.
     */
    private static MethodHandle maskedLoop(MethodHandle function)
    {
        // There are 4 loop clauses
        //   1. selectedPositionCount: constant int -- exits the loop when index is >= to selected position
        //   2. selectedPositions: constant int[]
        //   3. ---- execute input function
        //   4. index: an int incremented on each step
        // Each loop clause, can define a variable before the loop, and then within the loop, each clause can
        // update the variable, and then check if the loop should be updated. Each clause is executed in order
        // in the body. This is why index is tested in the first clause, and updated in the last clause.
        //
        // All update and test method, must start with the parameters: int selectedPositionCount, int[] selectedPositions, int index

        MethodHandle selectedPositionStopCondition = dropArguments(INT_GREATER_THAN, 1, int[].class);

        MethodHandle arrayElementGetter = MethodHandles.arrayElementGetter(int[].class);
        function = collectArguments(function, 0, arrayElementGetter);
        function = dropArguments(function, 2, AggregationMask.class);
        function = dropArguments(function, 0, int.class);

        MethodHandle increment = dropArguments(INT_INCREMENT, 0, int.class, int[].class);

        MethodHandle loop = MethodHandles.loop(
                new MethodHandle[]{MASK_GET_SELECTED_POSITION_COUNT, null, selectedPositionStopCondition},
                new MethodHandle[]{MASK_GET_SELECTED_POSITIONS},
                new MethodHandle[]{null, function},
                new MethodHandle[]{null, increment});

        return loop;
    }

    private static MethodHandle preprocessRleParameter(MethodHandle rleMethodHandle, int parameterIndex)
    {
        // read the rleValue from a RunLengthEncodedBlock
        rleMethodHandle = collectArguments(
                rleMethodHandle,
                parameterIndex,
                RLE_GET_VALUE);
        rleMethodHandle = explicitCastArguments(rleMethodHandle, rleMethodHandle.type().changeParameterType(parameterIndex, Block.class));
        return rleMethodHandle;
    }

    /**
     * For inner loop of RLE parameter, hard code the position to 0.
     */
    private static MethodHandle processRleParameter(int parameterStart, MethodHandle function)
    {
        return MethodHandles.insertArguments(function, parameterStart + 1, 0);
    }

    /**
     * Outside of loop for RLE parameter, replace the RLE block with the RLE value
     */
    private static MethodHandle preprocessDictionaryParameter(MethodHandle dictionaryMethodHandle, int dictionaryParameterIndex)
    {
        // starting method handle type
        // (int position, otherParams..., Block dictionary, int[] rawIds, int rawIdsOffset

        // read the dictionary, rawIds, and rawIdsOffset from a DictionaryBlock
        // (int position, otherParams..., DictionaryBlock block, DictionaryBlock block, DictionaryBlock block
        dictionaryMethodHandle = collectArguments(dictionaryMethodHandle, dictionaryParameterIndex, DICTIONARY_GET_DICTIONARY);
        dictionaryMethodHandle = collectArguments(dictionaryMethodHandle, dictionaryParameterIndex + 1, DICTIONARY_GET_RAW_IDS);
        dictionaryMethodHandle = collectArguments(dictionaryMethodHandle, dictionaryParameterIndex + 2, DICTIONARY_GET_RAW_IDS_OFFSET);

        // consolidate the 3 dictionary block parameters into one
        // (int position, otherParams..., DictionaryBlock block)
        int[] reorder = IntStream.range(0, dictionaryMethodHandle.type().parameterCount())
                .map(i -> i < dictionaryParameterIndex + 2 ? i : i - 2)
                .toArray();
        reorder[dictionaryParameterIndex + 1] = dictionaryParameterIndex;
        reorder[dictionaryParameterIndex + 2] = dictionaryParameterIndex;
        MethodType newType = dictionaryMethodHandle.type().dropParameterTypes(dictionaryParameterIndex, dictionaryParameterIndex + 2);
        dictionaryMethodHandle = permuteArguments(dictionaryMethodHandle, newType, reorder);
        dictionaryMethodHandle = explicitCastArguments(dictionaryMethodHandle, dictionaryMethodHandle.type().changeParameterType(dictionaryParameterIndex, Block.class));
        return dictionaryMethodHandle;
    }

    /**
     * For inner loop of dictionary parameter, replace positions with {@code rawIds[position + rawIdsOffset]}
     */
    private static MethodHandle processDictionaryParameter(int parameterStart, MethodHandle function)
    {
        function = collectArguments(function, parameterStart + 1, MethodHandles.arrayElementGetter(int[].class));
        function = collectArguments(function, parameterStart + 2, INT_ADD_FUNCTION);
        MethodHandle methodHandle = mergeParameterWithPositionArgument(parameterStart + 2, function);
        return methodHandle;
    }

    /**
     * Outside of loop for RLE parameter, replace the Dictionary block with three parameters: the dictionary, rawIds array, and rawIdsOffset.
     */
    private static MethodHandle processBasicParameter(int parameterStart, MethodHandle function)
    {
        return mergeParameterWithPositionArgument(parameterStart + 1, function);
    }

    /**
     * Merges the specified parameter with the loop position argument, which is always the first parameter For example,
     * <blockquote><pre>{@code
     * (int position, Block a, int aPosition, Block b, int bPositions)
     * }</pre></blockquote>
     * with {@code parameterToMerge} set to 2, returns:
     * <blockquote><pre>{@code
     * (int position, Block a, Block b, int bPositions)
     * }</pre></blockquote>
     */
    private static MethodHandle mergeParameterWithPositionArgument(int parameterToMerge, MethodHandle methodHandle)
    {
        int[] reorder = IntStream.range(0, methodHandle.type().parameterCount())
                .map(i -> i < parameterToMerge + 1 ? i : i - 1)
                .toArray();
        reorder[parameterToMerge] = 0;
        MethodType newType = methodHandle.type().dropParameterTypes(parameterToMerge, parameterToMerge + 1);
        return permuteArguments(methodHandle, newType, reorder);
    }

    /**
     * Returns a method handle that test if the specified position of the methodType is of the specified block type.
     */
    private static MethodHandle blockParameterIndexOf(MethodType methodType, int position, Class<? extends Block> blockType)
    {
        verify(methodType.parameterType(position).equals(Block.class));
        MethodHandle instanceOf = IS_INSTANCE.bindTo(blockType);
        instanceOf = dropArguments(instanceOf, 0, methodType.parameterList().subList(0, position));
        instanceOf = dropArguments(instanceOf, position + 1, methodType.parameterList().subList(position + 1, methodType.parameterCount()));
        instanceOf = explicitCastArguments(instanceOf, methodType.changeReturnType(boolean.class));
        return instanceOf;
    }

    // helper methods used for method handle combinations
    private static int add(int left, int right)
    {
        return left + right;
    }

    private static int increment(int index)
    {
        return index + 1;
    }

    private static boolean greaterThan(int selectedPositionCount, int index)
    {
        return selectedPositionCount > index;
    }

    private enum BlockType
    {
        RLE, DICTIONARY, BASIC
    }
}
