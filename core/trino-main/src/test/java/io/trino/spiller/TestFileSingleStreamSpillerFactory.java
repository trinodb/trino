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
package io.trino.spiller;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Closer;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import io.trino.spi.Page;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.BlockEncodingSerde;
import io.trino.spi.block.TestingBlockEncodingSerde;
import io.trino.spi.type.Type;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;

import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.MoreFiles.listFiles;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static com.google.common.util.concurrent.Futures.getUnchecked;
import static io.trino.execution.buffer.CompressionCodec.NONE;
import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spiller.FileSingleStreamSpillerFactory.SPILL_FILE_PREFIX;
import static io.trino.spiller.FileSingleStreamSpillerFactory.SPILL_FILE_SUFFIX;
import static java.nio.file.Files.setPosixFilePermissions;
import static java.util.Collections.emptyList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_METHOD;

@TestInstance(PER_METHOD)
public class TestFileSingleStreamSpillerFactory
{
    private final BlockEncodingSerde blockEncodingSerde = new TestingBlockEncodingSerde();
    private Closer closer;
    private ListeningExecutorService executor;
    private File spillPath1;
    private File spillPath2;

    @BeforeEach
    public void setUp()
            throws IOException
    {
        closer = Closer.create();
        executor = MoreExecutors.listeningDecorator(Executors.newCachedThreadPool());
        closer.register(() -> executor.shutdownNow());
        spillPath1 = Files.createTempDirectory("tmp_spill_path1").toFile();
        closer.register(() -> deleteRecursively(spillPath1.toPath(), ALLOW_INSECURE));
        spillPath2 = Files.createTempDirectory("tmp_spill_path2").toFile();
        closer.register(() -> deleteRecursively(spillPath2.toPath(), ALLOW_INSECURE));
    }

    @AfterEach
    public void tearDown()
            throws Exception
    {
        closer.close();
        closer = null;
    }

    @Test
    public void testDistributesSpillOverPaths()
            throws Exception
    {
        List<Type> types = ImmutableList.of(BIGINT);
        List<Path> spillPaths = ImmutableList.of(spillPath1.toPath(), spillPath2.toPath());
        FileSingleStreamSpillerFactory spillerFactory = spillerFactoryFactory(spillPaths);

        assertThat(listFiles(spillPath1.toPath())).isEmpty();
        assertThat(listFiles(spillPath2.toPath())).isEmpty();

        Page page = buildPage();
        List<SingleStreamSpiller> spillers = new ArrayList<>();
        for (int i = 0; i < 10; ++i) {
            SingleStreamSpiller singleStreamSpiller = spillerFactory.create(types, bytes -> {}, newSimpleAggregatedMemoryContext().newLocalMemoryContext("test"));
            getUnchecked(singleStreamSpiller.spill(page));
            spillers.add(singleStreamSpiller);
        }
        assertThat(listFiles(spillPath1.toPath())).hasSize(5);
        assertThat(listFiles(spillPath2.toPath())).hasSize(5);

        spillers.forEach(SingleStreamSpiller::close);
        assertThat(listFiles(spillPath1.toPath())).isEmpty();
        assertThat(listFiles(spillPath2.toPath())).isEmpty();
    }

    @Test
    public void testDistributesSpillOverPathsBadDisk()
            throws Exception
    {
        List<Type> types = ImmutableList.of(BIGINT);
        List<Path> spillPaths = ImmutableList.of(spillPath1.toPath(), spillPath2.toPath());
        FileSingleStreamSpillerFactory spillerFactory = spillerFactoryFactory(spillPaths);

        assertThat(listFiles(spillPath1.toPath())).isEmpty();
        assertThat(listFiles(spillPath2.toPath())).isEmpty();

        // Set first spiller path to read-only after initialization to emulate a disk failing during runtime
        setPosixFilePermissions(spillPath1.toPath(), ImmutableSet.of(PosixFilePermission.OWNER_READ));

        Page page = buildPage();
        List<SingleStreamSpiller> spillers = new ArrayList<>();
        int numberOfSpills = 10;
        for (int i = 0; i < numberOfSpills; ++i) {
            SingleStreamSpiller singleStreamSpiller = spillerFactory.create(types, bytes -> {}, newSimpleAggregatedMemoryContext().newLocalMemoryContext("test"));
            getUnchecked(singleStreamSpiller.spill(page));
            spillers.add(singleStreamSpiller);
        }

        // bad disk should receive no spills, with the good disk taking the remainder
        assertThat(listFiles(spillPath1.toPath())).isEmpty();
        assertThat(listFiles(spillPath2.toPath())).hasSize(numberOfSpills);

        spillers.forEach(SingleStreamSpiller::close);
        assertThat(listFiles(spillPath1.toPath())).isEmpty();
        assertThat(listFiles(spillPath2.toPath())).isEmpty();
    }

    private Page buildPage()
    {
        BlockBuilder col1 = BIGINT.createFixedSizeBlockBuilder(1);
        BIGINT.writeLong(col1, 42);
        return new Page(col1.build());
    }

    @Test
    public void throwsIfNoDiskSpace()
    {
        List<Type> types = ImmutableList.of(BIGINT);
        List<Path> spillPaths = ImmutableList.of(spillPath1.toPath(), spillPath2.toPath());
        FileSingleStreamSpillerFactory spillerFactory = spillerFactoryFactory(spillPaths, 0.0);

        assertThatThrownBy(() -> spillerFactory.create(types, bytes -> {}, newSimpleAggregatedMemoryContext().newLocalMemoryContext("test")))
                .isInstanceOf(RuntimeException.class)
                .hasMessage("No free or healthy space available for spill");
    }

    @Test
    public void throwIfNoSpillPaths()
    {
        List<Path> spillPaths = emptyList();
        List<Type> types = ImmutableList.of(BIGINT);
        FileSingleStreamSpillerFactory spillerFactory = spillerFactoryFactory(spillPaths);

        assertThatThrownBy(() -> spillerFactory.create(types, bytes -> {}, newSimpleAggregatedMemoryContext().newLocalMemoryContext("test")))
                .isInstanceOf(RuntimeException.class)
                .hasMessage("No spill paths configured");
    }

    @Test
    public void testCleanupOldSpillFiles()
            throws Exception
    {
        List<Path> spillPaths = ImmutableList.of(spillPath1.toPath(), spillPath2.toPath());
        spillPath1.mkdirs();
        spillPath2.mkdirs();

        java.nio.file.Files.createTempFile(spillPath1.toPath(), SPILL_FILE_PREFIX, SPILL_FILE_SUFFIX);
        java.nio.file.Files.createTempFile(spillPath1.toPath(), SPILL_FILE_PREFIX, SPILL_FILE_SUFFIX);
        java.nio.file.Files.createTempFile(spillPath1.toPath(), SPILL_FILE_PREFIX, "blah");
        java.nio.file.Files.createTempFile(spillPath2.toPath(), SPILL_FILE_PREFIX, SPILL_FILE_SUFFIX);
        java.nio.file.Files.createTempFile(spillPath2.toPath(), "blah", SPILL_FILE_SUFFIX);
        java.nio.file.Files.createTempFile(spillPath2.toPath(), "blah", "blah");

        assertThat(listFiles(spillPath1.toPath())).hasSize(3);
        assertThat(listFiles(spillPath2.toPath())).hasSize(3);

        FileSingleStreamSpillerFactory spillerFactory = spillerFactoryFactory(spillPaths);
        spillerFactory.cleanupOldSpillFiles();

        assertThat(listFiles(spillPath1.toPath())).hasSize(1);
        assertThat(listFiles(spillPath2.toPath())).hasSize(2);
    }

    @Test
    public void testCacheInvalidatedOnBadDisk()
            throws Exception
    {
        List<Type> types = ImmutableList.of(BIGINT);
        List<Path> spillPaths = ImmutableList.of(spillPath1.toPath(), spillPath2.toPath());

        FileSingleStreamSpillerFactory spillerFactory = spillerFactoryFactory(spillPaths);

        assertThat(listFiles(spillPath1.toPath())).isEmpty();
        assertThat(listFiles(spillPath2.toPath())).isEmpty();

        Page page = buildPage();
        List<SingleStreamSpiller> spillers = new ArrayList<>();

        SingleStreamSpiller singleStreamSpiller = spillerFactory.create(types, bytes -> {}, newSimpleAggregatedMemoryContext().newLocalMemoryContext("test"));
        getUnchecked(singleStreamSpiller.spill(page));
        spillers.add(singleStreamSpiller);

        SingleStreamSpiller singleStreamSpiller2 = spillerFactory.create(types, bytes -> {}, newSimpleAggregatedMemoryContext().newLocalMemoryContext("test"));
        // Set second spiller path to read-only after initialization to emulate a disk failing during runtime
        setPosixFilePermissions(spillPath2.toPath(), ImmutableSet.of(PosixFilePermission.OWNER_READ));

        assertThatThrownBy(() -> getUnchecked(singleStreamSpiller2.spill(page)))
                .isInstanceOf(com.google.common.util.concurrent.UncheckedExecutionException.class)
                .hasMessageContaining("Failed to spill pages");
        spillers.add(singleStreamSpiller2);

        assertThat(spillerFactory.getSpillPathCacheSize())
                .describedAs("cache still contains entries")
                .isEqualTo(0);

        // restore permissions to allow cleanup
        setPosixFilePermissions(spillPath2.toPath(), ImmutableSet.of(PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_WRITE, PosixFilePermission.OWNER_EXECUTE));
        spillers.forEach(SingleStreamSpiller::close);
        assertThat(listFiles(spillPath1.toPath())).isEmpty();
        assertThat(listFiles(spillPath2.toPath())).isEmpty();
    }

    @Test
    public void testCacheFull()
            throws Exception
    {
        List<Type> types = ImmutableList.of(BIGINT);
        List<Path> spillPaths = ImmutableList.of(spillPath1.toPath(), spillPath2.toPath());

        FileSingleStreamSpillerFactory spillerFactory = spillerFactoryFactory(spillPaths);

        assertThat(listFiles(spillPath1.toPath())).isEmpty();
        assertThat(listFiles(spillPath2.toPath())).isEmpty();

        Page page = buildPage();
        List<SingleStreamSpiller> spillers = new ArrayList<>();

        SingleStreamSpiller singleStreamSpiller = spillerFactory.create(types, bytes -> {}, newSimpleAggregatedMemoryContext().newLocalMemoryContext("test"));
        getUnchecked(singleStreamSpiller.spill(page));
        spillers.add(singleStreamSpiller);

        SingleStreamSpiller singleStreamSpiller2 = spillerFactory.create(types, bytes -> {}, newSimpleAggregatedMemoryContext().newLocalMemoryContext("test"));
        getUnchecked(singleStreamSpiller2.spill(page));
        spillers.add(singleStreamSpiller2);

        assertThat(spillerFactory.getSpillPathCacheSize())
                .describedAs("cache contains no entries")
                .isEqualTo(2);

        spillers.forEach(SingleStreamSpiller::close);
        assertThat(listFiles(spillPath1.toPath())).isEmpty();
        assertThat(listFiles(spillPath2.toPath())).isEmpty();
    }

    private FileSingleStreamSpillerFactory spillerFactoryFactory(List<Path> paths)
    {
        return spillerFactoryFactory(paths, 1.0);
    }

    private FileSingleStreamSpillerFactory spillerFactoryFactory(List<Path> paths, Double maxUsedSpaceThreshold)
    {
        return new FileSingleStreamSpillerFactory(
                executor, // executor won't be closed, because we don't call destroy() on the spiller factory
                blockEncodingSerde,
                new SpillerStats(),
                paths,
                maxUsedSpaceThreshold,
                NONE,
                false);
    }
}
