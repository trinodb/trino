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
package io.trino.plugin.paimon.fileio;

import io.trino.filesystem.FileIterator;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoInput;
import io.trino.filesystem.TrinoInputFile;
import io.trino.filesystem.TrinoInputStream;
import io.trino.filesystem.TrinoOutputFile;
import io.trino.filesystem.local.LocalFileSystem;
import io.trino.filesystem.memory.MemoryFileSystem;
import io.trino.memory.context.AggregatedMemoryContext;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.TwoPhaseOutputStream;
import org.assertj.core.api.ThrowableAssert.ThrowingCallable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileAlreadyExistsException;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestPaimonFileIO
{
    @Test
    public void testObjectStoreDetectionUsesPaimonSchemes()
    {
        TrinoFileSystem fileSystem = new MemoryFileSystem();

        assertThat(new PaimonFileIO(fileSystem, new Path("s3://bucket/warehouse")).isObjectStore()).isTrue();
        assertThat(new PaimonFileIO(fileSystem, new Path("abfs://container/warehouse")).isObjectStore()).isTrue();
        assertThat(new PaimonFileIO(fileSystem, new Path("gs://bucket/warehouse")).isObjectStore()).isTrue();
        assertThat(new PaimonFileIO(fileSystem, new Path("cosn://bucket/warehouse")).isObjectStore()).isTrue();
        assertThat(new PaimonFileIO(fileSystem, new Path("file:///warehouse")).isObjectStore()).isFalse();
    }

    @Test
    public void testConstructorRequiresPath()
    {
        assertThatThrownBy(() -> new PaimonFileIO(new MemoryFileSystem(), null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("path is null");
    }

    @Test
    public void testFileIOLoaderRejectsNullDependencies()
    {
        assertThatThrownBy(() -> new PaimonFileIOLoader(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("trinoFileSystem is null");
        assertThatThrownBy(() -> new PaimonFileIOLoader(new MemoryFileSystem()).load(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("path is null");
    }

    @Test
    public void testObjectStoreMkdirsCreatesDirectoryMarker()
            throws IOException
    {
        PaimonFileIO fileIO = objectStoreFileIO(new MemoryFileSystem());
        Path databasePath = new Path("memory:///warehouse/minio_smoke.db");

        assertThat(fileIO.exists(databasePath)).isFalse();

        assertThat(fileIO.mkdirs(databasePath)).isTrue();

        assertThat(fileIO.exists(databasePath)).isTrue();
        assertThat(fileIO.getFileStatus(databasePath).isDir()).isTrue();
        assertThat(fileIO.listStatus(databasePath)).isEmpty();
    }

    @Test
    public void testObjectStoreCheckOrMkdirsToleratesFailedHeadForMissingDirectoryPrefix()
            throws IOException
    {
        PaimonFileIO fileIO = objectStoreFileIO(new MissingDirectoryPrefixHeadFileSystem());
        Path warehousePath = new Path("memory:///warehouse");
        Path databasePath = new Path(warehousePath, "minio_smoke.db");

        assertThat(fileIO.exists(warehousePath)).isFalse();
        assertThat(fileIO.exists(databasePath)).isFalse();

        fileIO.checkOrMkdirs(warehousePath);
        assertThat(fileIO.exists(warehousePath)).isTrue();
        assertThat(fileIO.getFileStatus(warehousePath).isDir()).isTrue();

        assertThat(fileIO.mkdirs(databasePath)).isTrue();
        assertThat(fileIO.exists(databasePath)).isTrue();
        assertThat(fileIO.getFileStatus(databasePath).isDir()).isTrue();
    }

    @Test
    public void testObjectStoreDirectoryOperationsTolerateFailedHeadForMissingDirectoryPrefix()
            throws IOException
    {
        PaimonFileIO fileIO = objectStoreFileIO(new MissingDirectoryPrefixHeadFileSystem());
        Path warehousePath = new Path("memory:///warehouse");
        Path databasePath = new Path(warehousePath, "minio_smoke.db");
        Path sourceFile = new Path(warehousePath, ".schema-0.tmp");
        Path targetFile = new Path(databasePath, sourceFile.getName());

        fileIO.checkOrMkdirs(warehousePath);
        assertThat(fileIO.mkdirs(databasePath)).isTrue();

        assertThat(fileIO.listDirectories(warehousePath))
                .singleElement()
                .satisfies(status -> {
                    assertThat(status.isDir()).isTrue();
                    assertThat(status.getPath().toString()).isEqualTo(databasePath.toString());
                });

        fileIO.writeFile(sourceFile, "schema", false);
        assertThat(fileIO.rename(sourceFile, databasePath)).isTrue();
        assertThat(fileIO.exists(sourceFile)).isFalse();
        assertThat(fileIO.readFileUtf8(targetFile)).isEqualTo("schema");

        assertThatThrownBy(() -> fileIO.delete(databasePath, false))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("is not empty");
        assertThat(fileIO.delete(databasePath, true)).isTrue();
        assertThat(fileIO.exists(databasePath)).isFalse();
    }

    @Test
    public void testObjectStoreFileProbeFailurePropagatesWhenPathHasNoDirectoryEvidence()
    {
        Path path = new Path("memory:///warehouse/minio_smoke.db/orders/schema-0");
        PaimonFileIO fileIO = objectStoreFileIO(new UnavailableHeadFileSystem(path));

        assertThatThrownBy(() -> fileIO.exists(path))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("simulated S3 HEAD outage");
        assertThatThrownBy(() -> fileIO.getFileStatus(path))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("simulated S3 HEAD outage");
        assertThatThrownBy(() -> fileIO.listStatus(path))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("simulated S3 HEAD outage");
        assertThatThrownBy(() -> fileIO.listDirectories(path))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("simulated S3 HEAD outage");
        assertThatThrownBy(() -> fileIO.checkOrMkdirs(path))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("simulated S3 HEAD outage");
    }

    @Test
    public void testObjectStoreIllegalFileProbeFailurePropagates()
            throws IOException
    {
        Path path = new Path("memory:///warehouse/minio_smoke.db/orders/schema-0");
        InvalidProbeFileSystem fileSystem = new InvalidProbeFileSystem(path);
        PaimonFileIO fileIO = objectStoreFileIO(fileSystem);
        fileSystem.newOutputFile(Location.of(path.toString())).createOrOverwrite("schema".getBytes(StandardCharsets.UTF_8));

        assertInvalidProbeFailure(() -> fileIO.exists(path), path);
        assertInvalidProbeFailure(() -> fileIO.getFileStatus(path), path);
        assertInvalidProbeFailure(() -> fileIO.listStatus(path), path);
        assertInvalidProbeFailure(() -> fileIO.listDirectories(path), path);
        assertInvalidProbeFailure(() -> fileIO.checkOrMkdirs(path), path);
        assertInvalidProbeFailure(() -> fileIO.delete(path, false), path);
        assertInvalidProbeFailure(() -> fileIO.rename(path, new Path("memory:///warehouse/minio_smoke.db/orders/schema-1")), path);
        assertInvalidProbeFailure(() -> fileIO.newOutputStream(path, false), path);
        assertInvalidProbeFailure(() -> fileIO.newTwoPhaseOutputStream(path, false), path);
    }

    @Test
    public void testObjectStoreNotFoundHeadDoesNotBreakFileOperations()
            throws IOException
    {
        PaimonFileIO fileIO = objectStoreFileIO(new MissingObjectHeadFileSystem());
        Path missing = new Path("memory:///warehouse/minio_smoke.db/orders/missing");
        Path source = new Path("memory:///warehouse/minio_smoke.db/orders/schema/.schema-0.tmp");
        Path target = new Path("memory:///warehouse/minio_smoke.db/orders/schema/schema-0");
        Path dataFile = new Path("memory:///warehouse/minio_smoke.db/orders/data/data-0.parquet");

        assertThat(fileIO.exists(missing)).isFalse();
        assertThat(fileIO.listStatus(missing)).isEmpty();
        assertThat(fileIO.delete(missing, false)).isFalse();
        assertThat(fileIO.rename(missing, target)).isFalse();

        fileIO.writeFile(source, "schema", false);
        assertThat(fileIO.rename(source, target)).isTrue();
        assertThat(fileIO.readFileUtf8(target)).isEqualTo("schema");

        TwoPhaseOutputStream out = fileIO.newTwoPhaseOutputStream(dataFile, false);
        out.write("data".getBytes(StandardCharsets.UTF_8));
        out.closeForCommit().commit(fileIO);

        assertThat(fileIO.readFileUtf8(dataFile)).isEqualTo("data");
    }

    @Test
    public void testListStatusReturnsOnlyDirectChildren()
            throws IOException
    {
        PaimonFileIO fileIO = objectStoreFileIO(new MemoryFileSystem());
        Path tablePath = new Path("memory:///warehouse/minio_smoke.db/orders");
        Path nestedPath = new Path(tablePath, "manifest");
        Path directFile = new Path(tablePath, "schema-0");
        Path nestedFile = new Path(nestedPath, "manifest-list-0");

        fileIO.mkdirs(tablePath);
        fileIO.mkdirs(nestedPath);
        fileIO.writeFile(directFile, "schema", false);
        fileIO.writeFile(nestedFile, "manifest", false);

        Map<String, Boolean> paths = Arrays.stream(fileIO.listStatus(tablePath))
                .collect(Collectors.toMap(status -> status.getPath().toString(), status -> status.isDir()));
        assertThat(paths).containsEntry(directFile.toString(), false);
        assertThat(paths).containsEntry(nestedPath.toString(), true);
        assertThat(paths).doesNotContainKey(nestedFile.toString());
    }

    @Test
    public void testObjectStoreExistingDirectoryWithoutMarkerIsDetectedFromChildDirectory()
            throws IOException
    {
        PaimonFileIO fileIO = objectStoreFileIO(new MemoryFileSystem());
        Path tablePath = new Path("memory:///warehouse/minio_smoke.db/orders");
        Path manifestPath = new Path(tablePath, "manifest");
        Path manifestFile = new Path(manifestPath, "manifest-list-0");

        fileIO.writeFile(manifestFile, "manifest", false);

        assertThat(fileIO.exists(tablePath)).isTrue();
        assertThat(fileIO.getFileStatus(tablePath).isDir()).isTrue();
        assertThat(fileIO.listStatus(tablePath))
                .singleElement()
                .satisfies(status -> {
                    assertThat(status.isDir()).isTrue();
                    assertThat(status.getPath().toString()).isEqualTo(manifestPath.toString());
                });
    }

    @Test
    public void testObjectStoreExistingDirectoryWithoutMarkerIsDetectedFromDirectFile()
            throws IOException
    {
        PaimonFileIO fileIO = objectStoreFileIO(new MemoryFileSystem());
        Path schemaPath = new Path("memory:///warehouse/minio_smoke.db/orders/schema");
        Path schemaFile = new Path(schemaPath, "schema-0");

        fileIO.writeFile(schemaFile, "schema", false);

        assertThat(fileIO.exists(schemaPath)).isTrue();
        assertThat(fileIO.getFileStatus(schemaPath).isDir()).isTrue();
        assertThat(fileIO.listStatus(schemaPath))
                .singleElement()
                .satisfies(status -> {
                    assertThat(status.isDir()).isFalse();
                    assertThat(status.getPath().toString()).isEqualTo(schemaFile.toString());
                });
    }

    @Test
    public void testListStatusOnFileReturnsFile()
            throws IOException
    {
        PaimonFileIO fileIO = objectStoreFileIO(new MemoryFileSystem());
        Path file = new Path("memory:///warehouse/minio_smoke.db/orders/schema-0");

        fileIO.writeFile(file, "schema", false);

        assertThat(fileIO.listStatus(file))
                .singleElement()
                .satisfies(status -> {
                    assertThat(status.isDir()).isFalse();
                    assertThat(status.getPath().toString()).isEqualTo(file.toString());
                    assertThat(status.getLen()).isEqualTo(6);
                });
    }

    @Test
    public void testListDirectoriesOnFileReturnsEmpty()
            throws IOException
    {
        PaimonFileIO fileIO = objectStoreFileIO(new MemoryFileSystem());
        Path file = new Path("memory:///warehouse/minio_smoke.db/orders/schema-0");

        fileIO.writeFile(file, "schema", false);

        assertThat(fileIO.listDirectories(file)).isEmpty();
    }

    @Test
    public void testListStatusOnFileDoesNotProbeFileAsDirectory()
            throws IOException
    {
        PaimonFileIO fileIO = objectStoreFileIO(new StrictDirectoryProbeFileSystem());
        Path file = new Path("memory:///warehouse/minio_smoke.db/orders/schema-0");

        fileIO.writeFile(file, "schema", false);

        assertThat(fileIO.exists(file)).isTrue();
        assertThat(fileIO.getFileStatus(file).isDir()).isFalse();
        assertThat(fileIO.listStatus(file))
                .singleElement()
                .satisfies(status -> {
                    assertThat(status.isDir()).isFalse();
                    assertThat(status.getPath().toString()).isEqualTo(file.toString());
                });
        assertThat(fileIO.listDirectories(file)).isEmpty();
    }

    @Test
    public void testNonRecursiveDeleteAllowsOnlyDirectoryMarker()
            throws IOException
    {
        PaimonFileIO fileIO = objectStoreFileIO(new MemoryFileSystem());
        Path emptyDirectory = new Path("memory:///warehouse/minio_smoke.db/empty_table");

        fileIO.mkdirs(emptyDirectory);

        assertThat(fileIO.delete(emptyDirectory, false)).isTrue();
        assertThat(fileIO.exists(emptyDirectory)).isFalse();
    }

    @Test
    public void testNonRecursiveDeleteFailsWhenDirectChildDirectoryExists()
            throws IOException
    {
        PaimonFileIO fileIO = objectStoreFileIO(new MemoryFileSystem());
        Path tablePath = new Path("memory:///warehouse/minio_smoke.db/orders");
        Path nestedPath = new Path(tablePath, "manifest");

        fileIO.mkdirs(tablePath);
        fileIO.mkdirs(nestedPath);

        assertThatThrownBy(() -> fileIO.delete(tablePath, false))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("is not empty");
        assertThat(fileIO.exists(tablePath)).isTrue();
        assertThat(fileIO.exists(nestedPath)).isTrue();
    }

    @Test
    public void testObjectStoreRenameFileFallsBackToCopyAndDelete()
            throws IOException
    {
        PaimonFileIO fileIO = objectStoreFileIO(new NoRenameFileSystem());
        Path source = new Path("memory:///warehouse/minio_smoke.db/orders/schema/.schema-0.tmp");
        Path target = new Path("memory:///warehouse/minio_smoke.db/orders/schema/schema-0");

        fileIO.writeFile(source, "schema", false);

        assertThat(fileIO.rename(source, target)).isTrue();

        assertThat(fileIO.exists(source)).isFalse();
        assertThat(fileIO.exists(target)).isTrue();
        assertThat(fileIO.readFileUtf8(target)).isEqualTo("schema");
    }

    @Test
    public void testObjectStoreOutputStreamRejectsExistingTargetWhenOverwriteDisabled()
            throws IOException
    {
        PaimonFileIO fileIO = objectStoreFileIO(new OverwritingCreateFileSystem());
        Path target = new Path("memory:///warehouse/minio_smoke.db/orders/schema/schema-0");

        fileIO.writeFile(target, "old-schema", false);

        assertThatThrownBy(() -> fileIO.writeFile(target, "new-schema", false))
                .isInstanceOf(FileAlreadyExistsException.class)
                .hasMessageContaining(target.toString());
        assertThat(fileIO.readFileUtf8(target)).isEqualTo("old-schema");
    }

    @Test
    public void testObjectStoreOutputStreamOverwriteDoesNotDeleteBeforeCreate()
            throws IOException
    {
        PaimonFileIO fileIO = objectStoreFileIO(new DeleteRejectingFileSystem());
        Path target = new Path("memory:///warehouse/minio_smoke.db/orders/schema/schema-0");

        fileIO.writeFile(target, "old-schema", false);

        fileIO.writeFile(target, "new-schema", true);

        assertThat(fileIO.readFileUtf8(target)).isEqualTo("new-schema");
    }

    @Test
    public void testObjectStoreOutputStreamOverwriteFallbackUsesByteArray()
            throws IOException
    {
        StreamingOverwriteFileSystem fileSystem = new StreamingOverwriteFileSystem();
        PaimonFileIO fileIO = objectStoreFileIO(fileSystem);
        Path target = new Path("memory:///warehouse/minio_smoke.db/orders/schema/schema-0");

        fileIO.writeFile(target, "old-schema", false);

        fileIO.writeFile(target, "new-schema", true);

        assertThat(fileIO.readFileUtf8(target)).isEqualTo("new-schema");
        assertThat(fileSystem.isByteArrayOverwriteCalled()).isTrue();
    }

    @Test
    public void testObjectStoreTwoPhaseOutputStreamCommitsWithoutRename()
            throws IOException
    {
        PaimonFileIO fileIO = objectStoreFileIO(new NoRenameFileSystem());
        Path target = new Path("memory:///warehouse/minio_smoke.db/orders/data/data-0.parquet");

        TwoPhaseOutputStream out = fileIO.newTwoPhaseOutputStream(target, false);
        out.write("data".getBytes(StandardCharsets.UTF_8));
        assertThat(out.getPos()).isEqualTo(4);

        TwoPhaseOutputStream.Committer committer = out.closeForCommit();
        committer.commit(fileIO);

        assertThat(committer.targetPath()).isEqualTo(target);
        assertThat(fileIO.readFileUtf8(target)).isEqualTo("data");
    }

    @Test
    public void testObjectStoreTwoPhaseOutputStreamDiscardDeletesTarget()
            throws IOException
    {
        PaimonFileIO fileIO = objectStoreFileIO(new NoRenameFileSystem());
        Path target = new Path("memory:///warehouse/minio_smoke.db/orders/data/data-0.parquet");

        TwoPhaseOutputStream out = fileIO.newTwoPhaseOutputStream(target, false);
        out.write("data".getBytes(StandardCharsets.UTF_8));

        TwoPhaseOutputStream.Committer committer = out.closeForCommit();
        assertThat(fileIO.exists(target)).isTrue();

        committer.discard(fileIO);

        assertThat(fileIO.exists(target)).isFalse();
    }

    @Test
    public void testObjectStoreTwoPhaseOutputStreamCloseWithoutCommitDeletesTarget()
            throws IOException
    {
        PaimonFileIO fileIO = objectStoreFileIO(new NoRenameFileSystem());
        Path target = new Path("memory:///warehouse/minio_smoke.db/orders/data/data-0.parquet");

        TwoPhaseOutputStream out = fileIO.newTwoPhaseOutputStream(target, false);
        out.write("data".getBytes(StandardCharsets.UTF_8));

        out.close();

        assertThat(fileIO.exists(target)).isFalse();
        assertThatThrownBy(out::closeForCommit)
                .isInstanceOf(IOException.class)
                .hasMessageContaining("already closed");
    }

    @Test
    public void testObjectStoreTwoPhaseOutputStreamCloseRetriesFailedTargetCleanup()
            throws IOException
    {
        DeleteFailingOnceFileSystem fileSystem = new DeleteFailingOnceFileSystem();
        PaimonFileIO fileIO = objectStoreFileIO(fileSystem);
        Path target = new Path("memory:///warehouse/minio_smoke.db/orders/data/data-0.parquet");

        TwoPhaseOutputStream out = fileIO.newTwoPhaseOutputStream(target, false);
        out.write("data".getBytes(StandardCharsets.UTF_8));

        assertThatThrownBy(out::close)
                .isInstanceOf(IOException.class)
                .hasMessage("transient delete failure for " + target);
        assertThat(fileIO.exists(target)).isTrue();
        assertThat(fileSystem.getDeleteCalls()).isEqualTo(1);

        out.close();

        assertThat(fileIO.exists(target)).isFalse();
        assertThat(fileSystem.getDeleteCalls()).isEqualTo(2);
        assertThatThrownBy(out::closeForCommit)
                .isInstanceOf(IOException.class)
                .hasMessageContaining("already closed");
    }

    @Test
    public void testObjectStoreTwoPhaseOutputStreamCloseRetriesFailedOutputCloseAfterCleanup()
            throws IOException
    {
        OutputCloseFailingOnceFileSystem fileSystem = new OutputCloseFailingOnceFileSystem();
        PaimonFileIO fileIO = objectStoreFileIO(fileSystem);
        Path target = new Path("memory:///warehouse/minio_smoke.db/orders/data/data-0.parquet");

        TwoPhaseOutputStream out = fileIO.newTwoPhaseOutputStream(target, false);
        out.write("data".getBytes(StandardCharsets.UTF_8));

        assertThatThrownBy(out::close)
                .isInstanceOf(IOException.class)
                .hasMessage("transient output close failure");
        assertThat(fileIO.exists(target)).isFalse();
        assertThat(fileSystem.getDeleteCalls()).isEqualTo(1);

        out.close();

        assertThat(fileIO.exists(target)).isFalse();
        assertThat(fileSystem.getDeleteCalls()).isEqualTo(1);
        assertThatThrownBy(out::closeForCommit)
                .isInstanceOf(IOException.class)
                .hasMessageContaining("already closed");
    }

    @Test
    public void testObjectStoreTwoPhaseOutputStreamCloseAfterCloseForCommitKeepsTarget()
            throws IOException
    {
        PaimonFileIO fileIO = objectStoreFileIO(new NoRenameFileSystem());
        Path target = new Path("memory:///warehouse/minio_smoke.db/orders/data/data-0.parquet");

        TwoPhaseOutputStream out = fileIO.newTwoPhaseOutputStream(target, false);
        out.write("data".getBytes(StandardCharsets.UTF_8));

        TwoPhaseOutputStream.Committer committer = out.closeForCommit();
        out.close();
        committer.commit(fileIO);

        assertThat(fileIO.readFileUtf8(target)).isEqualTo("data");
        assertThatThrownBy(out::closeForCommit)
                .isInstanceOf(IOException.class)
                .hasMessageContaining("already closed");
    }

    @Test
    public void testObjectStoreTwoPhaseOutputStreamRejectsExistingTarget()
            throws IOException
    {
        PaimonFileIO fileIO = objectStoreFileIO(new NoRenameFileSystem());
        Path target = new Path("memory:///warehouse/minio_smoke.db/orders/data/data-0.parquet");

        fileIO.writeFile(target, "old-data", false);

        assertThatThrownBy(() -> fileIO.newTwoPhaseOutputStream(target, false))
                .isInstanceOf(FileAlreadyExistsException.class)
                .hasMessageContaining(target.toString());
        assertThat(fileIO.readFileUtf8(target)).isEqualTo("old-data");
    }

    @Test
    public void testObjectStoreTwoPhaseOutputStreamRejectsExistingTargetWhenOverwriteEnabled()
            throws IOException
    {
        PaimonFileIO fileIO = objectStoreFileIO(new DeleteRejectingFileSystem());
        Path target = new Path("memory:///warehouse/minio_smoke.db/orders/data/data-0.parquet");

        fileIO.writeFile(target, "old-data", false);

        assertThatThrownBy(() -> fileIO.newTwoPhaseOutputStream(target, true))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Object-store two-phase overwrite is not supported")
                .hasMessageContaining(target.toString());
        assertThat(fileIO.readFileUtf8(target)).isEqualTo("old-data");
    }

    @Test
    public void testObjectStoreRenameMissingFileReturnsFalse()
            throws IOException
    {
        PaimonFileIO fileIO = objectStoreFileIO(new NoRenameFileSystem());
        Path source = new Path("memory:///warehouse/minio_smoke.db/orders/schema/.schema-0.tmp");
        Path target = new Path("memory:///warehouse/minio_smoke.db/orders/schema/schema-0");

        assertThat(fileIO.rename(source, target)).isFalse();
        assertThat(fileIO.exists(source)).isFalse();
        assertThat(fileIO.exists(target)).isFalse();
    }

    @Test
    public void testObjectStoreRenameFileToExistingTargetReturnsFalse()
            throws IOException
    {
        PaimonFileIO fileIO = objectStoreFileIO(new NoRenameFileSystem());
        Path source = new Path("memory:///warehouse/minio_smoke.db/orders/schema/.schema-0.tmp");
        Path target = new Path("memory:///warehouse/minio_smoke.db/orders/schema/schema-0");

        fileIO.writeFile(source, "new-schema", false);
        fileIO.writeFile(target, "old-schema", false);

        assertThat(fileIO.rename(source, target)).isFalse();
        assertThat(fileIO.readFileUtf8(source)).isEqualTo("new-schema");
        assertThat(fileIO.readFileUtf8(target)).isEqualTo("old-schema");
    }

    @Test
    public void testObjectStoreRenameFileToExistingDirectory()
            throws IOException
    {
        PaimonFileIO fileIO = objectStoreFileIO(new NoRenameFileSystem());
        Path source = new Path("memory:///warehouse/minio_smoke.db/orders/_temporary/.schema-0.tmp");
        Path targetDirectory = new Path("memory:///warehouse/minio_smoke.db/orders/schema");
        Path target = new Path(targetDirectory, source.getName());

        fileIO.writeFile(source, "schema", false);
        fileIO.mkdirs(targetDirectory);

        assertThat(fileIO.rename(source, targetDirectory)).isTrue();
        assertThat(fileIO.exists(source)).isFalse();
        assertThat(fileIO.readFileUtf8(target)).isEqualTo("schema");
    }

    @Test
    public void testObjectStoreRenameMarkerDirectoryFails()
            throws IOException
    {
        PaimonFileIO fileIO = objectStoreFileIO(new NoRenameFileSystem());
        Path source = new Path("memory:///warehouse/minio_smoke.db/orders");
        Path target = new Path("memory:///warehouse/minio_smoke.db/orders_renamed");

        fileIO.mkdirs(source);

        assertThatThrownBy(() -> fileIO.rename(source, target))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("does not support directory renames");
        assertThat(fileIO.exists(source)).isTrue();
        assertThat(fileIO.exists(target)).isFalse();
    }

    @Test
    public void testObjectStoreRenameRealDirectoryFails()
            throws IOException
    {
        PaimonFileIO fileIO = objectStoreFileIO(new NoRenameFileSystem());
        Path source = new Path("memory:///warehouse/minio_smoke.db/orders");
        Path target = new Path("memory:///warehouse/minio_smoke.db/orders_renamed");

        fileIO.mkdirs(source);
        fileIO.writeFile(new Path(source, "schema-0"), "schema", false);

        assertThatThrownBy(() -> fileIO.rename(source, target))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("does not support directory renames");
        assertThat(fileIO.exists(source)).isTrue();
        assertThat(fileIO.exists(target)).isFalse();
    }

    @Test
    public void testObjectStoreRenameDirectoryFailsEvenWhenTargetExists()
            throws IOException
    {
        PaimonFileIO fileIO = objectStoreFileIO(new NoRenameFileSystem());
        Path source = new Path("memory:///warehouse/minio_smoke.db/orders");
        Path target = new Path("memory:///warehouse/minio_smoke.db/orders_renamed");

        fileIO.mkdirs(source);
        fileIO.mkdirs(target);

        assertThatThrownBy(() -> fileIO.rename(source, target))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("does not support directory renames");
        assertThat(fileIO.exists(source)).isTrue();
        assertThat(fileIO.exists(target)).isTrue();
    }

    @Test
    public void testLocalRenameMissingFileReturnsFalse(@TempDir java.nio.file.Path tempDirectory)
            throws IOException
    {
        PaimonFileIO fileIO = localFileIO(tempDirectory);
        Path source = new Path("local:///warehouse/orders/schema/.schema-0.tmp");
        Path target = new Path("local:///warehouse/orders/schema/schema-0");

        assertThat(fileIO.rename(source, target)).isFalse();
        assertThat(fileIO.exists(source)).isFalse();
        assertThat(fileIO.exists(target)).isFalse();
    }

    @Test
    public void testLocalRenameFileToExistingTargetReturnsFalse(@TempDir java.nio.file.Path tempDirectory)
            throws IOException
    {
        PaimonFileIO fileIO = localFileIO(tempDirectory);
        Path source = new Path("local:///warehouse/orders/schema/.schema-0.tmp");
        Path target = new Path("local:///warehouse/orders/schema/schema-0");

        fileIO.writeFile(source, "new-schema", false);
        fileIO.writeFile(target, "old-schema", false);

        assertThat(fileIO.rename(source, target)).isFalse();
        assertThat(fileIO.readFileUtf8(source)).isEqualTo("new-schema");
        assertThat(fileIO.readFileUtf8(target)).isEqualTo("old-schema");
    }

    @Test
    public void testLocalRenameFileToExistingDirectory(@TempDir java.nio.file.Path tempDirectory)
            throws IOException
    {
        PaimonFileIO fileIO = localFileIO(tempDirectory);
        Path source = new Path("local:///warehouse/orders/_temporary/.schema-0.tmp");
        Path targetDirectory = new Path("local:///warehouse/orders/schema");
        Path target = new Path(targetDirectory, source.getName());

        fileIO.writeFile(source, "schema", false);
        fileIO.mkdirs(targetDirectory);

        assertThat(fileIO.rename(source, targetDirectory)).isTrue();
        assertThat(fileIO.exists(source)).isFalse();
        assertThat(fileIO.readFileUtf8(target)).isEqualTo("schema");
    }

    @Test
    public void testLocalRenameDirectoryToExistingDirectory(@TempDir java.nio.file.Path tempDirectory)
            throws IOException
    {
        PaimonFileIO fileIO = localFileIO(tempDirectory);
        Path source = new Path("local:///warehouse/orders/schema");
        Path sourceFile = new Path(source, "schema-0");
        Path targetDirectory = new Path("local:///warehouse/orders_renamed");
        Path target = new Path(targetDirectory, source.getName());

        fileIO.writeFile(sourceFile, "schema", false);
        fileIO.mkdirs(targetDirectory);

        assertThat(fileIO.rename(source, targetDirectory)).isTrue();
        assertThat(fileIO.exists(source)).isFalse();
        assertThat(fileIO.exists(target)).isTrue();
        assertThat(fileIO.readFileUtf8(new Path(target, "schema-0"))).isEqualTo("schema");
    }

    private static PaimonFileIO objectStoreFileIO(TrinoFileSystem fileSystem)
    {
        return new PaimonFileIO(fileSystem, new Path("s3://bucket/warehouse"));
    }

    private static PaimonFileIO localFileIO(java.nio.file.Path tempDirectory)
    {
        return new PaimonFileIO(new LocalFileSystem(tempDirectory), new Path("local:///warehouse"));
    }

    private static void assertInvalidProbeFailure(ThrowingCallable callable, Path path)
    {
        assertThatThrownBy(callable)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("invalid file probe for " + path);
    }

    private static class NoRenameFileSystem
            implements TrinoFileSystem
    {
        private final MemoryFileSystem delegate = new MemoryFileSystem();

        @Override
        public TrinoInputFile newInputFile(Location location)
        {
            return delegate.newInputFile(location);
        }

        @Override
        public TrinoInputFile newInputFile(Location location, long length)
        {
            return delegate.newInputFile(location, length);
        }

        @Override
        public TrinoInputFile newInputFile(Location location, long length, Instant lastModified)
        {
            return delegate.newInputFile(location, length, lastModified);
        }

        @Override
        public TrinoOutputFile newOutputFile(Location location)
        {
            return delegate.newOutputFile(location);
        }

        @Override
        public void deleteFile(Location location)
                throws IOException
        {
            delegate.deleteFile(location);
        }

        @Override
        public void deleteDirectory(Location location)
                throws IOException
        {
            delegate.deleteDirectory(location);
        }

        @Override
        public void deleteFiles(Collection<Location> locations)
                throws IOException
        {
            delegate.deleteFiles(locations);
        }

        @Override
        public void renameFile(Location source, Location target)
                throws IOException
        {
            throw new IOException("S3 does not support renames");
        }

        @Override
        public FileIterator listFiles(Location location)
                throws IOException
        {
            return delegate.listFiles(location);
        }

        @Override
        public Optional<Boolean> directoryExists(Location location)
                throws IOException
        {
            return delegate.directoryExists(location);
        }

        @Override
        public void createDirectory(Location location)
                throws IOException
        {
            delegate.createDirectory(location);
        }

        @Override
        public void renameDirectory(Location source, Location target)
                throws IOException
        {
            delegate.renameDirectory(source, target);
        }

        @Override
        public Set<Location> listDirectories(Location location)
                throws IOException
        {
            return delegate.listDirectories(location);
        }

        @Override
        public Optional<Location> createTemporaryDirectory(Location targetPath, String temporaryPrefix, String relativePrefix)
                throws IOException
        {
            return delegate.createTemporaryDirectory(targetPath, temporaryPrefix, relativePrefix);
        }
    }

    private static class StrictFileListingFileSystem
            extends NoRenameFileSystem
    {
        @Override
        public FileIterator listFiles(Location location)
                throws IOException
        {
            if (newInputFile(location).exists()) {
                throw new IOException("Cannot list files under regular file: " + location);
            }
            return super.listFiles(location);
        }

        @Override
        public Set<Location> listDirectories(Location location)
                throws IOException
        {
            if (newInputFile(location).exists()) {
                throw new IOException("Cannot list directories under regular file: " + location);
            }
            return super.listDirectories(location);
        }
    }

    private static class OverwritingCreateFileSystem
            extends NoRenameFileSystem
    {
        @Override
        public TrinoOutputFile newOutputFile(Location location)
        {
            TrinoOutputFile delegate = super.newOutputFile(location);
            return new TrinoOutputFile()
            {
                @Override
                public void createOrOverwrite(byte[] data)
                        throws IOException
                {
                    delegate.createOrOverwrite(data);
                }

                @Override
                public OutputStream create(AggregatedMemoryContext memoryContext)
                {
                    return new ByteArrayOutputStream()
                    {
                        @Override
                        public void close()
                                throws IOException
                        {
                            delegate.createOrOverwrite(toByteArray());
                        }
                    };
                }

                @Override
                public Location location()
                {
                    return delegate.location();
                }
            };
        }
    }

    private static class StreamingOverwriteFileSystem
            extends NoRenameFileSystem
    {
        private boolean byteArrayOverwriteCalled;

        boolean isByteArrayOverwriteCalled()
        {
            return byteArrayOverwriteCalled;
        }

        @Override
        public TrinoOutputFile newOutputFile(Location location)
        {
            TrinoOutputFile delegate = super.newOutputFile(location);
            return new TrinoOutputFile()
            {
                @Override
                public void createOrOverwrite(byte[] data)
                        throws IOException
                {
                    byteArrayOverwriteCalled = true;
                    delegate.createOrOverwrite(data);
                }

                @Override
                public OutputStream create(AggregatedMemoryContext memoryContext)
                        throws IOException
                {
                    return delegate.create(memoryContext);
                }

                @Override
                public Location location()
                {
                    return delegate.location();
                }
            };
        }
    }

    private static class DeleteRejectingFileSystem
            extends NoRenameFileSystem
    {
        @Override
        public void deleteFile(Location location)
                throws IOException
        {
            throw new IOException("deleteFile should not be called for " + location);
        }
    }

    private static class DeleteFailingOnceFileSystem
            extends NoRenameFileSystem
    {
        private int deleteCalls;

        @Override
        public void deleteFile(Location location)
                throws IOException
        {
            deleteCalls++;
            if (deleteCalls == 1) {
                throw new IOException("transient delete failure for " + location);
            }
            super.deleteFile(location);
        }

        private int getDeleteCalls()
        {
            return deleteCalls;
        }
    }

    private static class OutputCloseFailingOnceFileSystem
            extends NoRenameFileSystem
    {
        private int deleteCalls;
        private boolean closeFailureThrown;

        @Override
        public TrinoOutputFile newOutputFile(Location location)
        {
            TrinoOutputFile delegate = super.newOutputFile(location);
            return new TrinoOutputFile()
            {
                @Override
                public void createOrOverwrite(byte[] data)
                        throws IOException
                {
                    delegate.createOrOverwrite(data);
                }

                @Override
                public OutputStream create(AggregatedMemoryContext memoryContext)
                        throws IOException
                {
                    OutputStream delegateStream = delegate.create(memoryContext);
                    return new OutputStream()
                    {
                        @Override
                        public void write(int b)
                                throws IOException
                        {
                            delegateStream.write(b);
                        }

                        @Override
                        public void write(byte[] bytes, int off, int len)
                                throws IOException
                        {
                            delegateStream.write(bytes, off, len);
                        }

                        @Override
                        public void close()
                                throws IOException
                        {
                            delegateStream.close();
                            if (!closeFailureThrown) {
                                closeFailureThrown = true;
                                throw new IOException("transient output close failure");
                            }
                        }
                    };
                }

                @Override
                public Location location()
                {
                    return delegate.location();
                }
            };
        }

        @Override
        public void deleteFile(Location location)
                throws IOException
        {
            deleteCalls++;
            super.deleteFile(location);
        }

        private int getDeleteCalls()
        {
            return deleteCalls;
        }
    }

    private static class StrictDirectoryProbeFileSystem
            extends StrictFileListingFileSystem
    {
        @Override
        public Optional<Boolean> directoryExists(Location location)
                throws IOException
        {
            if (newInputFile(location).exists()) {
                throw new IOException("Cannot check directory for regular file: " + location);
            }
            return super.directoryExists(location);
        }
    }

    private static class MissingDirectoryPrefixHeadFileSystem
            extends NoRenameFileSystem
    {
        @Override
        public TrinoInputFile newInputFile(Location location)
        {
            TrinoInputFile delegate = super.newInputFile(location);
            if (location.path().equals("warehouse") || location.path().endsWith(".db")) {
                return new FailedHeadInputFile(delegate, true);
            }
            return delegate;
        }
    }

    private static class UnavailableHeadFileSystem
            extends NoRenameFileSystem
    {
        private final Path failedPath;

        private UnavailableHeadFileSystem(Path failedPath)
        {
            this.failedPath = failedPath;
        }

        @Override
        public TrinoInputFile newInputFile(Location location)
        {
            TrinoInputFile delegate = super.newInputFile(location);
            if (location.toString().equals(failedPath.toString())) {
                return new FailedHeadInputFile(delegate, false);
            }
            return delegate;
        }
    }

    private static class MissingObjectHeadFileSystem
            extends NoRenameFileSystem
    {
        @Override
        public TrinoInputFile newInputFile(Location location)
        {
            return new MissingObjectHeadInputFile(super.newInputFile(location));
        }
    }

    private static class InvalidProbeFileSystem
            extends NoRenameFileSystem
    {
        private final Path failedPath;

        private InvalidProbeFileSystem(Path failedPath)
        {
            this.failedPath = failedPath;
        }

        @Override
        public TrinoInputFile newInputFile(Location location)
        {
            TrinoInputFile delegate = super.newInputFile(location);
            if (location.toString().equals(failedPath.toString())) {
                return new InvalidProbeInputFile(delegate);
            }
            return delegate;
        }
    }

    private static class InvalidProbeInputFile
            implements TrinoInputFile
    {
        private final TrinoInputFile delegate;

        private InvalidProbeInputFile(TrinoInputFile delegate)
        {
            this.delegate = delegate;
        }

        @Override
        public TrinoInput newInput()
                throws IOException
        {
            return delegate.newInput();
        }

        @Override
        public TrinoInputStream newStream()
                throws IOException
        {
            return delegate.newStream();
        }

        @Override
        public long length()
                throws IOException
        {
            return delegate.length();
        }

        @Override
        public Instant lastModified()
                throws IOException
        {
            return delegate.lastModified();
        }

        @Override
        public boolean exists()
        {
            throw new IllegalArgumentException("invalid file probe for " + delegate.location());
        }

        @Override
        public Location location()
        {
            return delegate.location();
        }
    }

    private static class MissingObjectHeadInputFile
            implements TrinoInputFile
    {
        private final TrinoInputFile delegate;

        private MissingObjectHeadInputFile(TrinoInputFile delegate)
        {
            this.delegate = delegate;
        }

        @Override
        public TrinoInput newInput()
                throws IOException
        {
            return delegate.newInput();
        }

        @Override
        public TrinoInputStream newStream()
                throws IOException
        {
            return delegate.newStream();
        }

        @Override
        public long length()
                throws IOException
        {
            return delegate.length();
        }

        @Override
        public Instant lastModified()
                throws IOException
        {
            return delegate.lastModified();
        }

        @Override
        public boolean exists()
                throws IOException
        {
            if (!delegate.exists()) {
                throw new FileNotFoundException("simulated S3 HEAD not found for " + delegate.location());
            }
            return true;
        }

        @Override
        public Location location()
        {
            return delegate.location();
        }
    }

    private static class FailedHeadInputFile
            implements TrinoInputFile
    {
        private final TrinoInputFile delegate;
        private final boolean notFound;

        private FailedHeadInputFile(TrinoInputFile delegate, boolean notFound)
        {
            this.delegate = delegate;
            this.notFound = notFound;
        }

        @Override
        public TrinoInput newInput()
                throws IOException
        {
            return delegate.newInput();
        }

        @Override
        public TrinoInputStream newStream()
                throws IOException
        {
            return delegate.newStream();
        }

        @Override
        public long length()
                throws IOException
        {
            return delegate.length();
        }

        @Override
        public Instant lastModified()
                throws IOException
        {
            return delegate.lastModified();
        }

        @Override
        public boolean exists()
                throws IOException
        {
            if (notFound) {
                throw new FileNotFoundException("simulated S3 HEAD not found for " + delegate.location());
            }
            throw new IOException("simulated S3 HEAD outage for " + delegate.location());
        }

        @Override
        public Location location()
        {
            return delegate.location();
        }
    }
}
