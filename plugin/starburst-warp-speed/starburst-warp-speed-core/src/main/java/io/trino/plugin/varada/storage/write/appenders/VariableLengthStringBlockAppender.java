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
package io.trino.plugin.varada.storage.write.appenders;

import io.airlift.slice.Slice;
import io.trino.plugin.varada.dictionary.WriteDictionary;
import io.trino.plugin.varada.dispatcher.model.WarmUpElement;
import io.trino.plugin.varada.dispatcher.model.WarmUpElementState;
import io.trino.plugin.varada.juffer.BlockPosHolder;
import io.trino.plugin.varada.storage.engine.StorageEngineConstants;
import io.trino.plugin.varada.storage.juffers.ExtendedJuffer;
import io.trino.plugin.varada.storage.juffers.WriteJuffersWarmUpElement;
import io.trino.plugin.varada.storage.write.WarmupElementStats;
import io.trino.plugin.varada.util.SliceUtils;
import io.trino.plugin.varada.warmup.exceptions.WarmupException;
import io.trino.spi.type.Type;

import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.nio.ShortBuffer;
import java.util.function.Function;

/**
 * Called for VARCHAR(n) with n>8
 */
public class VariableLengthStringBlockAppender
        extends DataBlockAppender
{
    private final int weRecTypeLength;
    private final Type filterType;
    private final byte[] extRecordHeaderBuff; // fixed zeroed buffer to append in extended records at the end
    private final StorageEngineConstants storageEngineConstants;
    private final int warmDataVarcharMaxLengthWithDictionary;
    private final int warmDataVarcharMaxLength;

    public VariableLengthStringBlockAppender(WriteJuffersWarmUpElement juffersWE,
            StorageEngineConstants storageEngineConstants,
            int weRecTypeLength,
            Type filterType,
            int warmDataVarcharMaxLength)
    {
        super(juffersWE);
        this.storageEngineConstants = storageEngineConstants;
        this.weRecTypeLength = weRecTypeLength;
        this.filterType = filterType;
        this.warmDataVarcharMaxLengthWithDictionary = storageEngineConstants.getVarcharMaxLen();
        this.warmDataVarcharMaxLength = Math.min(warmDataVarcharMaxLength, storageEngineConstants.getVarcharMaxLen());
        this.extRecordHeaderBuff = new byte[storageEngineConstants.getVarlenExtRecordHeaderSize() - 2 * Integer.BYTES];
    }

    @Override
    public AppendResult appendWithDictionary(BlockPosHolder blockPos, boolean stopAfterOneFlush, WriteDictionary writeDictionary, WarmupElementStats warmupElementStats)
    {
        ShortBuffer buff = (ShortBuffer) juffersWE.getRecordBuffer();
        Function<Slice, Slice> sliceConverter = SliceUtils.getSliceConverter(filterType, weRecTypeLength, false, true);
        int nullsCount = 0;
        if (blockPos.mayHaveNull()) {
            for (; blockPos.inRange(); blockPos.advance()) {
                if (blockPos.isNull()) {
                    buff.put(NULL_VALUE_BYTE_SIGNAL);
                    nullBuff.put(NULL_VALUE_BYTE_SIGNAL);
                    nullsCount++;
                }
                else {
                    Slice slice = getSlice(blockPos, sliceConverter);
                    warmupElementStats.updateMinMax(slice);
                    writeValue(writeDictionary, buff, slice);
                }
            }
        }
        else {
            for (; blockPos.inRange(); blockPos.advance()) {
                Slice slice = getSlice(blockPos, sliceConverter);
                warmupElementStats.updateMinMax(slice);
                writeValue(writeDictionary, buff, slice);
            }
        }
        return new AppendResult(nullsCount);
    }

    private Slice getSlice(BlockPosHolder blockPos, Function<Slice, Slice> sliceConverter)
    {
        Slice slice = blockPos.getSlice();
        if (slice.length() > warmDataVarcharMaxLengthWithDictionary) {
            // we do not warm data that has a string longer than max supported
            throw new WarmupException(
                    String.format("appendVarlenBlock with dictionary found a string length %d longer than max %d", slice.length(), warmDataVarcharMaxLengthWithDictionary),
                    WarmUpElementState.State.FAILED_PERMANENTLY);
        }
        return sliceConverter.apply(slice);
    }

    private void writeValue(WriteDictionary writeDictionary, ShortBuffer buff, Slice value)
    {
        short key = writeDictionary.get(value);
        juffersWE.updateRecordBufferProps(key, value.length());
        buff.put(key);
        advanceNullBuff();
    }

    @Override
    public AppendResult appendWithoutDictionary(int jufferPos, BlockPosHolder blockPos, boolean stopAfterOneFlush, WarmUpElement warmUpElement, WarmupElementStats warmupElementStats)
    {
        ByteBuffer recordBuff = (ByteBuffer) juffersWE.getRecordBuffer();
        int recBuffSize = juffersWE.getRecBuffSize();
        ExtendedJuffer extendedJuffers = juffersWE.getExtRecordJuffer();
        ByteBuffer extendedBuff = null;
        if (extendedJuffers != null) {
            extendedBuff = (ByteBuffer) extendedJuffers.getWrappedBuffer();
        }
        IntBuffer varlenMdBuff = (IntBuffer) juffersWE.getVarlenMdJuffer().getWrappedBuffer();
        int varlenMdBaseOffset = varlenMdBuff.get(varlenMdBuff.position()) - recordBuff.position();

        StringAtt att = new StringAtt();
        int nullsCount = 0;
        int nullsCountCommitted = 0;
        boolean isNull;
        Slice value;
        Function<Slice, Slice> sliceConverter = SliceUtils.getSliceConverter(filterType, weRecTypeLength, false, true);

        int recordsCommitted = 0;
        while (blockPos.inRange()) {
            isNull = blockPos.isNull();

            // 1. prepare attributes/value
            if (isNull) {
                att.recLen = 1;
                value = null;
            }
            else {
                value = blockPos.getSlice();
                if (value.length() > warmDataVarcharMaxLength) {
                    // we do not warm data that has a string longer than max supported
                    throw new WarmupException(
                            String.format("appendVarlenBlock found a string length %d longer than max %d", value.length(), warmDataVarcharMaxLength),
                            WarmUpElementState.State.FAILED_PERMANENTLY);
                }
                value = sliceConverter.apply(value);
                // handle variable length and change len, putLength and value accordingly
                getVarlenValueAtt(value.length(), att, storageEngineConstants);
            }

            // 2. handle end of page and end of buffer
            paddVarlenPageEnd(recordBuff, att);

            if (recordBuff.position() >= recBuffSize) {
                if (recordBuff.position() > recBuffSize) {
                    throw new WarmupException(
                            "appendVarlenBlock reached " + recordBuff.position() + " beyond recBuffSize " + recBuffSize,
                            WarmUpElementState.State.FAILED_PERMANENTLY);
                }
                // Intentionally not advancing the varlenMdAddress, so this last one will be overwritten by following calls
                varlenMdBaseOffset += recordBuff.position();
                varlenMdBuff.put(varlenMdBuff.position(), varlenMdBaseOffset);
                int numExtBytes = (extendedBuff != null) ? extendedBuff.position() : 0;
                int numRecs = jufferPos + blockPos.getPos();
                int addedNV = nullsCount - nullsCountCommitted;
                int position = recordBuff.position();

                juffersWE.commitAndResetWE(numRecs, addedNV, position, numExtBytes);
                recordsCommitted = blockPos.getPos();
                if (stopAfterOneFlush) {
                    return new AppendResult(nullsCount, recordsCommitted);
                }
                nullsCountCommitted = nullsCount;
            }

            // 3. actual layout if the record in the buffer + handling of extended records + update properties (crc/min-max/counters)
            int recStartPos = recordBuff.position();
            if (isNull) {
                nullsCount++;
                nullBuff.put(NULL_VALUE_BYTE_SIGNAL);
                recordBuff.put((byte) 1); // we put string length = 1
                recordBuff.put((byte) 1); // dictionary rollback in native puts 1s since it uses memset so we do the same
            }
            else {
                advanceNullBuff();
                warmupElementStats.updateMinMax(value);
                if (att.extLen > 0) {
                    // handle extended string
                    // update min-max
                    juffersWE.updateRecordBufferProps(SliceUtils.str2int(value, true), att.extLen);

                    // first check if we have space in the buffer, if not commit and reset
                    if (extendedBuff.position() + att.extLen >= extendedJuffers.getExtWESize()) {
                        extendedJuffers.commitAndResetExtRecordBuffer();
                    }
                    // remember the first one
                    extendedJuffers.updateExtRecordFirstOffset(varlenMdBaseOffset + recStartPos);
                    // update the linked list
                    if (extendedJuffers.getExtRecordLastPos() != 0) {
                        recordBuff.putInt(extendedJuffers.getExtRecordLastPos(), varlenMdBaseOffset + recStartPos);
                        extendedJuffers.advancedExtRecordLastPos(0);
                    }

                    // put extended record mark
                    recordBuff.put((byte) storageEngineConstants.getVarlenExtMark());
                    // put record header in the record buffer
                    recordBuff.putInt(att.extLen);
                    juffersWE.advancedExtRecordLastPos(recordBuff.position());
                    recordBuff.putInt(-1); // invalid offset for the extended records list. might be overridden by the offset of the next one
                    recordBuff.put(extRecordHeaderBuff);
                    // put extended part in the extended buffer
                    extendedBuff.put(value.toByteBuffer(att.recLen, att.extLen));
                }
                else {
                    // create byte buffer to put in the record buffer and for crc calculation (single value optimization)
                    ByteBuffer byteBuffer = value.toByteBuffer(0, att.recLen);
                    // update min-max and single value optimization
                    juffersWE.updateRecordBufferProps(SliceUtils.str2int(value, true), byteBuffer, att.recLen, recStartPos);
                    // put length and value
                    recordBuff.put((byte) att.recLen);
                    recordBuff.put(byteBuffer);
                }
            }

            // 4. update varlen metadata and bytes count
            if (((jufferPos + blockPos.getPos()) & (storageEngineConstants.getVarlenMdGranularity() - 1)) == 0) {
                varlenMdBuff.put(varlenMdBaseOffset + recStartPos);
            }

            // 5. advance to next value
            blockPos.advance();
        }

        if (recordBuff.position() < recBuffSize) {
            // as long as we did not put anything else, this is the end of the page. if we add more we will override
            recordBuff.put(recordBuff.position(), (byte) storageEngineConstants.getVarlenMarkEnd());
        }

        // Intentionally not advancing the varlenMdAddress, so this last one will be overwritten by following calls
        varlenMdBuff.put(varlenMdBuff.position(), varlenMdBaseOffset + recordBuff.position());
        return new AppendResult(nullsCount, recordsCommitted);
    }

    private void paddVarlenPageEnd(ByteBuffer recordBuff, StringAtt att)
    {
        // we assume that all buffers are page aligned so even if the position here is global the page alignment is the same
        int currPos = recordBuff.position();
        int endPos = currPos + att.recLen + att.recSuffix;
        long currPage = currPos & storageEngineConstants.getPageSizeMask();
        long endPage = endPos & storageEngineConstants.getPageSizeMask(); // Checking for endAddress and not "-1" since we add the len byte

        // advance to beginning of next page if we are splitted - both char and varchar
        if (currPage != endPage) {
            int paddingSize = storageEngineConstants.getPageSize() - (currPos & storageEngineConstants.getPageOffsetMask());

            //put the markEnd byte
            recordBuff.put((byte) storageEngineConstants.getVarlenMarkEnd());
            //pad the rest - paddingSize was calculated with the markEnd included that's why the (-1)
            padBuffer(recordBuff, paddingSize - 1);
        }
    }

    // check if we have an extention and set the attributes accordingly
    private void getVarlenValueAtt(int valueLength, StringAtt att, StorageEngineConstants storageEngineConstants)
    {
        if (valueLength > storageEngineConstants.getVarlenExtLimit()) {
            att.recLen = 0;
            att.recSuffix = storageEngineConstants.getVarlenExtRecordHeaderSize();
            att.extLen = valueLength;
        }
        else {
            att.recLen = valueLength;
            att.recSuffix = 0;
            att.extLen = 0;
        }
    }

    public static class StringAtt
    {
        int recLen;                  // length of the record value
        int recSuffix;               // length of record suffix, can be zero
        int extLen;                  // length of the extended part if exists without the header

        StringAtt()
        {
            this.recLen = 0;
            this.extLen = 0;
            this.recSuffix = 0;
        }

        @Override
        public String toString()
        {
            return "StringAtt{" +
                    "recLen=" + recLen +
                    ", recSuffix=" + recSuffix +
                    ", extLen=" + extLen +
                    '}';
        }
    }
}
