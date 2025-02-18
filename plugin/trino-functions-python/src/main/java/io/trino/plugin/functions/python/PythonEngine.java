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
package io.trino.plugin.functions.python;

import com.dylibso.chicory.runtime.ExportFunction;
import com.dylibso.chicory.runtime.HostFunction;
import com.dylibso.chicory.runtime.ImportValues;
import com.dylibso.chicory.runtime.Instance;
import com.dylibso.chicory.runtime.Memory;
import com.dylibso.chicory.wasi.WasiOptions;
import com.dylibso.chicory.wasi.WasiPreview1;
import com.dylibso.chicory.wasm.ChicoryException;
import com.dylibso.chicory.wasm.WasmModule;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Closer;
import com.google.common.jimfs.Configuration;
import com.google.common.jimfs.Jimfs;
import io.airlift.log.Logger;
import io.airlift.slice.BasicSliceInput;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceInput;
import io.airlift.slice.Slices;
import io.airlift.units.DataSize;
import io.trino.spi.ErrorCodeSupplier;
import io.trino.spi.StandardErrorCode;
import io.trino.spi.TrinoException;
import io.trino.spi.type.Type;
import io.trino.wasm.python.PythonModule;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static com.dylibso.chicory.wasm.types.ValueType.I32;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.trino.plugin.functions.python.TrinoTypes.binaryToJava;
import static io.trino.plugin.functions.python.TrinoTypes.javaToBinary;
import static io.trino.plugin.functions.python.TrinoTypes.toRowTypeDescriptor;
import static io.trino.plugin.functions.python.TrinoTypes.toTypeDescriptor;
import static io.trino.spi.StandardErrorCode.FUNCTION_IMPLEMENTATION_ERROR;
import static java.lang.Math.min;
import static java.lang.Math.toIntExact;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;

final class PythonEngine
        implements Closeable
{
    private static final Logger log = Logger.get(PythonEngine.class);
    private static final com.dylibso.chicory.log.Logger logger = JdkLogger.get(PythonEngine.class);

    private static final Configuration FS_CONFIG = Configuration.unix().toBuilder()
            .setAttributeViews("unix")
            .setMaxSize(DataSize.of(8, MEGABYTE).toBytes())
            .build();

    private static final Map<Integer, ErrorCodeSupplier> ERROR_CODES = Stream.of(StandardErrorCode.values())
            .collect(toImmutableMap(error -> error.toErrorCode().getCode(), identity()));

    private static final WasmModule PYTHON_MODULE = PythonModule.load();

    private final Closer closer = Closer.create();
    private final LimitedOutputStream stderr = new LimitedOutputStream();
    private final ExportFunction allocate;
    private final ExportFunction deallocate;
    private final ExportFunction setup;
    private final ExportFunction execute;
    private final Memory memory;
    private Type returnType;
    private List<Type> argumentTypes;
    private TrinoException error;

    public PythonEngine(String guestCode)
    {
        FileSystem fileSystem = closer.register(Jimfs.newFileSystem(FS_CONFIG));
        Path guestRoot = fileSystem.getPath("/guest");

        try {
            Files.createDirectories(guestRoot);
            Files.writeString(guestRoot.resolve("guest.py"), guestCode);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        OutputStream stdout = closer.register(new LoggingOutputStream(log));

        WasiOptions wasiOptions = WasiOptions.builder()
                .withStdout(stdout)
                .withStderr(stderr)
                .withDirectory(guestRoot.toString(), guestRoot)
                .build();

        WasiPreview1 wasi = closer.register(WasiPreview1.builder()
                .withLogger(logger)
                .withOptions(wasiOptions)
                .build());

        ImportValues importValues = ImportValues.builder()
                .addFunction(wasi.toHostFunctions())
                .addFunction(returnErrorHostFunction())
                .build();

        Instance instance = Instance.builder(PYTHON_MODULE)
                .withMachineFactory(PythonModule::create)
                .withImportValues(importValues)
                .build();

        allocate = instance.export("allocate");
        deallocate = instance.export("deallocate");
        setup = instance.export("setup");
        execute = instance.export("execute");
        memory = instance.memory();
    }

    public void setup(Type returnType, List<Type> argumentTypes, String handlerName)
    {
        try {
            doSetup(returnType, argumentTypes, handlerName);
        }
        catch (ChicoryException e) {
            throw fatalError("Python error", e);
        }
    }

    private void doSetup(Type returnType, List<Type> argumentTypes, String handlerName)
    {
        byte[] nameBytes = handlerName.getBytes(UTF_8);
        int nameAddress = allocate(nameBytes.length + 1);
        memory.write(nameAddress, nameBytes);
        memory.writeByte(nameAddress + nameBytes.length, (byte) 0);

        Slice argumentTypeSlice = toRowTypeDescriptor(argumentTypes);
        int argTypeAddress = allocate(argumentTypeSlice.length());
        writeSliceTo(argumentTypeSlice, argTypeAddress);

        Slice returnTypeSlice = toTypeDescriptor(returnType);
        int returnTypeAddress = allocate(returnTypeSlice.length());
        writeSliceTo(returnTypeSlice, returnTypeAddress);

        setup.apply(nameAddress, argTypeAddress, returnTypeAddress);

        deallocate(nameAddress);

        this.returnType = requireNonNull(returnType, "returnType is null");
        this.argumentTypes = ImmutableList.copyOf(requireNonNull(argumentTypes, "argumentTypes is null"));
    }

    private void writeSliceTo(Slice slice, int address)
    {
        memory.write(address, slice.byteArray(), slice.byteArrayOffset(), slice.length());
    }

    private int allocate(int size)
    {
        return toIntExact(allocate.apply(size)[0]);
    }

    private void deallocate(int address)
    {
        deallocate.apply(address);
    }

    private int execute(int address)
    {
        return toIntExact(execute.apply(address)[0]);
    }

    public Object execute(Object[] arguments)
    {
        Slice slice = javaToBinary(argumentTypes, arguments);
        int argAddress = allocate(slice.length());
        writeSliceTo(slice, argAddress);

        error = null;

        int resultAddress;
        try {
            resultAddress = execute(argAddress);
        }
        catch (ChicoryException e) {
            throw fatalError("Failed to invoke Python function", e);
        }

        deallocate(argAddress);

        if (error != null) {
            throw new TrinoException(error::getErrorCode, error.getMessage(), error.getCause());
        }

        if (resultAddress == 0) {
            throw new TrinoException(FUNCTION_IMPLEMENTATION_ERROR, "Python function did not return a result");
        }

        int resultSize = memory.readInt(resultAddress);
        byte[] bytes = memory.readBytes(resultAddress + 4, resultSize);
        deallocate(resultAddress);

        SliceInput input = new BasicSliceInput(Slices.wrappedBuffer(bytes));
        return binaryToJava(returnType, input);
    }

    public TrinoException fatalError(String message, ChicoryException e)
    {
        String error = stderr.toString(UTF_8).strip();
        if (!error.isEmpty()) {
            message += ":";
            message += error.contains("\n") ? "\n" : " ";
            message += error;
        }
        return new TrinoException(FUNCTION_IMPLEMENTATION_ERROR, message, e);
    }

    @Override
    public void close()
    {
        try {
            closer.close();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private long[] returnError(Instance instance, long... args)
    {
        int code = toIntExact(args[0]);
        int messageAddress = toIntExact(args[1]);
        int messageSize = toIntExact(args[2]);
        int tracebackAddress = toIntExact(args[3]);
        int tracebackSize = toIntExact(args[4]);

        Memory memory = instance.memory();
        String message = memory.readString(messageAddress, messageSize);

        Throwable traceback = null;
        if (tracebackAddress != 0) {
            String value = memory.readString(tracebackAddress, tracebackSize);
            traceback = new RuntimeException("Python traceback:\n" + value.stripTrailing());
        }

        ErrorCodeSupplier errorCode = ERROR_CODES.get(code);
        if (errorCode == null) {
            errorCode = FUNCTION_IMPLEMENTATION_ERROR;
            message = "Unknown error code (%s): %s".formatted(code, message);
        }

        error = new TrinoException(errorCode, message, traceback);

        return null;
    }

    private HostFunction returnErrorHostFunction()
    {
        return new HostFunction(
                "trino",
                "return_error",
                List.of(I32, I32, I32, I32, I32),
                List.of(),
                this::returnError);
    }

    @SuppressWarnings("UnsynchronizedOverridesSynchronized")
    private static class LimitedOutputStream
            extends ByteArrayOutputStream
    {
        private static final int LIMIT = 4096;

        @Override
        public void write(byte[] b, int off, int len)
        {
            if (count < LIMIT) {
                super.write(b, off, min(len, LIMIT - count));
            }
        }
    }
}
