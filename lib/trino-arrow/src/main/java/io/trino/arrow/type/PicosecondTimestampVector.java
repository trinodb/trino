package io.trino.arrow.type;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.util.hash.ArrowBufHasher;
import org.apache.arrow.vector.ExtensionTypeVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.complex.StructVector;

public class PicosecondTimestampVector extends ExtensionTypeVector<StructVector> {

    public PicosecondTimestampVector(String name, BufferAllocator allocator, StructVector underlyingVector) {
        super(name, allocator, underlyingVector);
    }

    @Override
    public Object getObject(int index) {
        return getUnderlyingVector().getObject(index);
    }

    @Override
    public int hashCode(int index) {
        return getUnderlyingVector().hashCode(index);
    }

    @Override
    public int hashCode(int index, ArrowBufHasher hasher) {
        return getUnderlyingVector().hashCode(index, hasher);
    }
}
