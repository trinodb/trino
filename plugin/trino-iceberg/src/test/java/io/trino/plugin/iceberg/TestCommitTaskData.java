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
package io.trino.plugin.iceberg;

import com.google.common.collect.ImmutableList;
import io.airlift.json.JsonCodec;
import io.trino.plugin.iceberg.CommitTaskData.DanglingDeleteFile;
import io.trino.plugin.iceberg.CommitTaskData.RewriteInfo;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static io.airlift.json.JsonCodec.jsonCodec;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestCommitTaskData
{
    private static final String DATA_FILE = "s3://bucket/data/part-0.parquet";
    private static final String DELETE_FILE = "s3://bucket/data/deletes-0.puffin";
    private static final String PARTITION_SPEC = "{\"spec-id\":0,\"fields\":[]}";

    private static final JsonCodec<DanglingDeleteFile> DANGLING_DELETE_CODEC = jsonCodec(DanglingDeleteFile.class);
    private static final JsonCodec<RewriteInfo> REWRITE_INFO_CODEC = jsonCodec(RewriteInfo.class);

    @Test
    void deletionVectorRequiresBothContentOffsetAndSize()
    {
        assertThatThrownBy(() -> new DanglingDeleteFile(
                DELETE_FILE,
                100L,
                1L,
                PARTITION_SPEC,
                Optional.empty(),
                /* contentOffset */ 0L, /* contentSizeInBytes */ null,
                DATA_FILE))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("contentOffset and contentSizeInBytes must both be present");

        assertThatThrownBy(() -> new DanglingDeleteFile(
                DELETE_FILE,
                100L,
                1L,
                PARTITION_SPEC,
                Optional.empty(),
                /* contentOffset */ null, /* contentSizeInBytes */ 32L,
                DATA_FILE))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("contentOffset and contentSizeInBytes must both be present");
    }

    @Test
    void positionDeleteFileHasNoContentOffsetOrSize()
    {
        DanglingDeleteFile positionDelete = new DanglingDeleteFile(
                DELETE_FILE,
                100L,
                1L,
                PARTITION_SPEC,
                Optional.empty(),
                null,
                null,
                DATA_FILE);

        assertThat(positionDelete.contentOffset()).isNull();
        assertThat(positionDelete.contentSizeInBytes()).isNull();
        assertThat(positionDelete.referencedDataFile()).isEqualTo(DATA_FILE);
    }

    @Test
    void deletionVectorRoundTripsThroughJson()
    {
        // Wire-shape regression guard: keeping nullable Long/String (not Optional) is a
        // deliberate compatibility choice; this test documents and protects that contract.
        DanglingDeleteFile original = new DanglingDeleteFile(
                DELETE_FILE,
                200L,
                5L,
                PARTITION_SPEC,
                Optional.of("{}"),
                42L,
                32L,
                DATA_FILE);

        DanglingDeleteFile roundTripped = DANGLING_DELETE_CODEC.fromJson(DANGLING_DELETE_CODEC.toJson(original));

        assertThat(roundTripped).isEqualTo(original);
    }

    @Test
    void positionDeleteRoundTripsThroughJson()
    {
        DanglingDeleteFile original = new DanglingDeleteFile(
                DELETE_FILE,
                100L,
                1L,
                PARTITION_SPEC,
                Optional.empty(),
                null,
                null,
                DATA_FILE);

        DanglingDeleteFile roundTripped = DANGLING_DELETE_CODEC.fromJson(DANGLING_DELETE_CODEC.toJson(original));

        assertThat(roundTripped).isEqualTo(original);
    }

    @Test
    void rejectsNullReferencedDataFileBecauseConsumerRequiresIt()
    {
        // IcebergMetadata.removeDanglingDeleteFiles unconditionally calls
        // FileMetadata.deleteFileBuilder().withReferencedDataFile(...). Enforce this
        // contract at the producer boundary instead of failing deep in the commit path.
        assertThatThrownBy(() -> new DanglingDeleteFile(
                DELETE_FILE,
                100L,
                1L,
                PARTITION_SPEC,
                Optional.empty(),
                null,
                null, /* referencedDataFile */ null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("referencedDataFile is null");
    }

    @Test
    void rejectsNegativeFileSizeOrRecordCount()
    {
        assertThatThrownBy(() -> new DanglingDeleteFile(
                DELETE_FILE, /* fileSizeInBytes */ -1L,
                1L,
                PARTITION_SPEC,
                Optional.empty(),
                null,
                null,
                DATA_FILE))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("fileSizeInBytes is negative");

        assertThatThrownBy(() -> new DanglingDeleteFile(
                DELETE_FILE,
                100L, /* recordCount */ -1L,
                PARTITION_SPEC,
                Optional.empty(),
                null,
                null,
                DATA_FILE))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("recordCount is negative");
    }

    @Test
    void rewriteInfoFreezesDanglingDeletesIntoImmutableList()
    {
        // Defensive copy: mutating the source list after construction must not leak into the handle.
        DanglingDeleteFile entry = new DanglingDeleteFile(
                DELETE_FILE,
                100L,
                1L,
                PARTITION_SPEC,
                Optional.empty(),
                null,
                null,
                DATA_FILE);
        List<DanglingDeleteFile> mutable = new ArrayList<>();
        mutable.add(entry);

        RewriteInfo rewriteInfo = new RewriteInfo(DATA_FILE, 1024L, 10L, IcebergFileFormat.PARQUET, mutable);

        // Mutating the source must not affect the record.
        mutable.clear();
        assertThat(rewriteInfo.danglingDeleteFiles()).containsExactly(entry);

        // The exposed list itself must reject mutation.
        assertThatThrownBy(() -> rewriteInfo.danglingDeleteFiles().clear())
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void rewriteInfoRoundTripsThroughJson()
    {
        RewriteInfo original = new RewriteInfo(
                DATA_FILE,
                2048L,
                12L,
                IcebergFileFormat.PARQUET,
                ImmutableList.of(new DanglingDeleteFile(
                        DELETE_FILE,
                        200L,
                        5L,
                        PARTITION_SPEC,
                        Optional.of("{}"),
                        42L,
                        32L,
                        DATA_FILE)));

        RewriteInfo roundTripped = REWRITE_INFO_CODEC.fromJson(REWRITE_INFO_CODEC.toJson(original));

        assertThat(roundTripped).isEqualTo(original);
    }
}
