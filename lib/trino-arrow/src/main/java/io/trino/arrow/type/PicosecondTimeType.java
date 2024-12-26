package io.trino.arrow.type;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;

import java.util.Arrays;

public class PicosecondTimeType extends ArrowType.ExtensionType {
    public static final String EXTENSION_NAME = "trino.time.pico";

    public PicosecondTimeType() {
        super();
    }

    public static java.util.List<Field> getStorageFields() {
        return Arrays.asList(
            new Field("nanos", FieldType.notNullable(new ArrowType.Int(64, true)), null),
            new Field("picoAdjustment", FieldType.notNullable(new ArrowType.Int(32, true)), null)
        );
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
        vector.addOrGet("nanos", FieldType.notNullable(new ArrowType.Int(64, true)), BigIntVector.class);
        vector.addOrGet("picoAdjustment", FieldType.notNullable(new ArrowType.Int(32, true)), IntVector.class);
        return vector;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        return getClass().hashCode();
    }
}
