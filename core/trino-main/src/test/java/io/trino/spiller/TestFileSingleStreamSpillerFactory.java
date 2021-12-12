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
import io.trino.spi.type.Type;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

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
import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.metadata.MetadataManager.createTestMetadataManager;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spiller.FileSingleStreamSpillerFactory.SPILL_FILE_PREFIX;
import static io.trino.spiller.FileSingleStreamSpillerFactory.SPILL_FILE_SUFFIX;
import static java.nio.file.Files.setPosixFilePermissions;
import static java.util.Collections.emptyList;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;

@Test(singleThreaded = true)
public class TestFileSingleStreamSpillerFactory
{
    private final BlockEncodingSerde blockEncodingSerde = createTestMetadataManager().getBlockEncodingSerde();
    private Closer closer;
    private ListeningExecutorService executor;
    private File spillPath1;
    private File spillPath2;

    @BeforeMethod
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

    @AfterMethod(alwaysRun = true)
    public void tearDown()
            throws Exception
    {
        closer.close();
    }

    @Test
    public void testDistributesSpillOverPaths()
            throws Exception
    {
        List<Type> types = ImmutableList.of(BIGINT);
        List<Path> spillPaths = ImmutableList.of(spillPath1.toPath(), spillPath2.toPath());
        FileSingleStreamSpillerFactory spillerFactory = spillerFactoryFactory(spillPaths);

        assertEquals(listFiles(spillPath1.toPath()).size(), 0);
        assertEquals(listFiles(spillPath2.toPath()).size(), 0);

        Page page = buildPage();
        List<SingleStreamSpiller> spillers = new ArrayList<>();
        for (int i = 0; i < 10; ++i) {
            SingleStreamSpiller singleStreamSpiller = spillerFactory.create(types, bytes -> {}, newSimpleAggregatedMemoryContext().newLocalMemoryContext("test"));
            getUnchecked(singleStreamSpiller.spill(page));
            spillers.add(singleStreamSpiller);
        }
        assertEquals(listFiles(spillPath1.toPath()).size(), 5);
        assertEquals(listFiles(spillPath2.toPath()).size(), 5);

        spillers.forEach(SingleStreamSpiller::close);
        assertEquals(listFiles(spillPath1.toPath()).size(), 0);
        assertEquals(listFiles(spillPath2.toPath()).size(), 0);
    }

    @Test
    public void testDistributesSpillOverPathsBadDisk()
            throws Exception
    {
        List<Type> types = ImmutableList.of(BIGINT);
        List<Path> spillPaths = ImmutableList.of(spillPath1.toPath(), spillPath2.toPath());
        FileSingleStreamSpillerFactory spillerFactory = spillerFactoryFactory(spillPaths);

        assertEquals(listFiles(spillPath1.toPath()).size(), 0);
        assertEquals(listFiles(spillPath2.toPath()).size(), 0);

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
        assertEquals(listFiles(spillPath1.toPath()).size(), 0);
        assertEquals(listFiles(spillPath2.toPath()).size(), numberOfSpills);

        spillers.forEach(SingleStreamSpiller::close);
        assertEquals(listFiles(spillPath1.toPath()).size(), 0);
        assertEquals(listFiles(spillPath2.toPath()).size(), 0);
    }

    private Page buildPage()
    {
        BlockBuilder col1 = BIGINT.createBlockBuilder(null, 1);
        col1.writeLong(42).closeEntry();
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

        assertEquals(listFiles(spillPath1.toPath()).size(), 3);
        assertEquals(listFiles(spillPath2.toPath()).size(), 3);

        FileSingleStreamSpillerFactory spillerFactory = spillerFactoryFactory(spillPaths);
        spillerFactory.cleanupOldSpillFiles();

        assertEquals(listFiles(spillPath1.toPath()).size(), 1);
        assertEquals(listFiles(spillPath2.toPath()).size(), 2);
    }

    @Test
    public void testCacheInvalidatedOnBadDisk()
            throws Exception
    {
        List<Type> types = ImmutableList.of(BIGINT);
        List<Path> spillPaths = ImmutableList.of(spillPath1.toPath(), spillPath2.toPath());

        FileSingleStreamSpillerFactory spillerFactory = spillerFactoryFactory(spillPaths);

        assertEquals(listFiles(spillPath1.toPath()).size(), 0);
        assertEquals(listFiles(spillPath2.toPath()).size(), 0);

        Page page = buildPage();
        List<SingleStreamSpiller> spillers = new ArrayList<>();

        SingleStreamSpiller singleStreamSpiller = spillerFactory.create(types, bytes -> {}, newSimpleAggregatedMemoryContext().newLocalMemoryContext("test"));
        getUnchecked(singleStreamSpiller.spill(page));
        spillers.add(singleStreamSpiller);

        SingleStreamSpiller singleStreamSpiller2 = spillerFactory.create(types, bytes -> {}, newSimpleAggregatedMemoryContext().newLocalMemoryContext("test"));
        // Set second spiller path to read-only after initialization to emulate a disk failing during runtime
        setPosixFilePermissions(spillPath2.toPath(), ImmutableSet.of(PosixFilePermission.OWNER_READ));

        assertThatThrownBy(() -> { getUnchecked(singleStreamSpiller2.spill(page)); })
                .isInstanceOf(com.google.common.util.concurrent.UncheckedExecutionException.class)
                .hasMessageContaining("Failed to spill pages");
        spillers.add(singleStreamSpiller2);

        assertEquals(spillerFactory.getSpillPathCacheSize(), 0, "cache still contains entries");

        // restore permissions to allow cleanup
        setPosixFilePermissions(spillPath2.toPath(), ImmutableSet.of(PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_WRITE, PosixFilePermission.OWNER_EXECUTE));
        spillers.forEach(SingleStreamSpiller::close);
        assertEquals(listFiles(spillPath1.toPath()).size(), 0);
        assertEquals(listFiles(spillPath2.toPath()).size(), 0);
    }

    @Test
    public void testCacheFull()
            throws Exception
    {
        List<Type> types = ImmutableList.of(BIGINT);
        List<Path> spillPaths = ImmutableList.of(spillPath1.toPath(), spillPath2.toPath());

        FileSingleStreamSpillerFactory spillerFactory = spillerFactoryFactory(spillPaths);

        assertEquals(listFiles(spillPath1.toPath()).size(), 0);
        assertEquals(listFiles(spillPath2.toPath()).size(), 0);

        Page page = buildPage();
        List<SingleStreamSpiller> spillers = new ArrayList<>();

        SingleStreamSpiller singleStreamSpiller = spillerFactory.create(types, bytes -> {}, newSimpleAggregatedMemoryContext().newLocalMemoryContext("test"));
        getUnchecked(singleStreamSpiller.spill(page));
        spillers.add(singleStreamSpiller);

        SingleStreamSpiller singleStreamSpiller2 = spillerFactory.create(types, bytes -> {}, newSimpleAggregatedMemoryContext().newLocalMemoryContext("test"));
        getUnchecked(singleStreamSpiller2.spill(page));
        spillers.add(singleStreamSpiller2);

        assertEquals(spillerFactory.getSpillPathCacheSize(), 2, "cache contains no entries");

        spillers.forEach(SingleStreamSpiller::close);
        assertEquals(listFiles(spillPath1.toPath()).size(), 0);
        assertEquals(listFiles(spillPath2.toPath()).size(), 0);
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
                false,
                false);
    }
}
