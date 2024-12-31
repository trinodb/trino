package io.trino.arrow.type;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.FieldType;

public class PicosecondTimeType extends ArrowType.ExtensionType {
    public static final String EXTENSION_NAME = "trino.time.pico";

    public PicosecondTimeType() {
        super();
    }


    @Override
    public String extensionName() {
        return EXTENSION_NAME;
    }

    @Override
    public ArrowType storageType() {
        return new ArrowType.Struct();
    }

    @Override
    public String serialize() {
        return "";
    }

    @Override
    public ArrowType deserialize(ArrowType storageType, String serializedData) {
        return new PicosecondTimeType();
    }

    @Override
    public boolean extensionEquals(ExtensionType other) {
        return other instanceof PicosecondTimeType;
    }

    @Override
    public FieldVector getNewVector(String name, FieldType fieldType, BufferAllocator allocator) {
        StructVector vector = new StructVector(name, allocator, fieldType, null);
        vector.addOrGet("time", FieldType.notNullable(new ArrowType.Time(TimeUnit.MICROSECOND, 64)), BigIntVector.class);
        vector.addOrGet("pico_adjustment", FieldType.notNullable(new ArrowType.Int(32, true)), IntVector.class);
        return new PicosecondTimeVector(name, allocator, vector);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        return o != null && getClass() == o.getClass();
    }

    @Override
    public int hashCode() {
        return getClass().hashCode();
    }
}
