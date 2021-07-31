package io.trino.server.protocol;

import io.trino.client.ClientStandardTypes;
import io.trino.client.ClientTypeSignature;
import io.trino.client.Column;
import io.vavr.Tuple2;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.Field;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.text.MessageFormat;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Utility to construct the arrow stream.
 *
 * @author pramanathan
 */
public class ArrowStreamUtil {

  private static final int MAX_PRECISION = 38;
  private static final int MAX_SCALE = 18;

  public String getArrowStream(List<Column> columns, Iterable<List<Object>> resultRows)
      throws Exception {
    String arrowStream = "";

    RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
    List<Field> fields = new ArrayList<>();
    List<FieldVector> fieldVectors = new ArrayList<>();

    try {
      Map<Integer,ClientTypeSignature> columnTypes = new HashMap<>();
      Integer columnCount = 0;
      for (Column column : columns) {
        FieldVector vector = this.getVector(column.getName(), column.getTypeSignature(), allocator);
        columnTypes.put(columnCount, column.getTypeSignature());
        fieldVectors.add(vector);
        fields.add(vector.getField());
        columnCount++;
      }

      int rowCount = 0;
      for (List<Object> row : resultRows) {
          columnCount = 0;
          for (Object cellData : row) {
            ClientTypeSignature fieldType = columnTypes.get(columnCount);
            this.putValue(rowCount, fieldVectors.get(columnCount), fieldType,cellData);
            columnCount++;
          }
          rowCount++;
      }

      for (FieldVector fieldVector : fieldVectors) {
        fieldVector.setValueCount(rowCount);
      }

      VectorSchemaRoot root = new VectorSchemaRoot(fields, fieldVectors);
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      try (ArrowStreamWriter writer = new ArrowStreamWriter(root, null, Channels.newChannel(out))) {
        writer.start();
        writer.writeBatch();
        writer.end();
      } catch (IOException e) {
        e.printStackTrace();
        throw new Exception("Failed to write the stream", e);
      }
      byte[] arrowStreamBytes = out.toByteArray();
      byte[] base64Encoded = Base64.getEncoder().encode(arrowStreamBytes);
      arrowStream = new String(base64Encoded, StandardCharsets.UTF_8);
      out = null;
    } finally {
      for (FieldVector fieldVector : fieldVectors) {
        if (fieldVector != null) {
          fieldVector.close();
        }
      }
      fieldVectors = null;
      allocator.close();
      allocator = null;
    }

    return arrowStream;
  }

  private FieldVector getVector(String fieldName, ClientTypeSignature fieldType, RootAllocator allocator) {
    FieldVector fieldVector = null;
    if (fieldType.getRawType().equals(ClientStandardTypes.VARCHAR)
        || fieldType.getRawType().equals(ClientStandardTypes.TIMESTAMP)
        || fieldType.getRawType().equals(ClientStandardTypes.DATE)
            || fieldType.getRawType().equals(ClientStandardTypes.CHAR)) {
      fieldVector = new VarCharVector(fieldName, allocator);
    } else if (fieldType.getRawType().equals(ClientStandardTypes.INTEGER)) {
      fieldVector = new IntVector(fieldName, allocator);
    } else if (fieldType.getRawType().equals(ClientStandardTypes.BIGINT)) {
      fieldVector = new BigIntVector(fieldName, allocator);
    } else if (fieldType.getRawType().equals(ClientStandardTypes.TINYINT)) {
      fieldVector = new TinyIntVector(fieldName, allocator);
    } else if (fieldType.getRawType().equals(ClientStandardTypes.SMALLINT)) {
      fieldVector = new SmallIntVector(fieldName, allocator);
    } else if (fieldType.getRawType().equals(ClientStandardTypes.DECIMAL)) {
      fieldVector =
          new DecimalVector(
              fieldName, allocator, MAX_PRECISION, MAX_SCALE);
    } else if (fieldType.getRawType().equals(ClientStandardTypes.REAL)) {
      fieldVector = new Float4Vector(fieldName, allocator);
    } else if (fieldType.getRawType().equals(ClientStandardTypes.DOUBLE)) {
      fieldVector = new Float8Vector(fieldName, allocator);
    } else if (fieldType.getRawType().equals(ClientStandardTypes.BOOLEAN)) {
      fieldVector = new BitVector(fieldName, allocator);
    } else {
      throw new RuntimeException(
          MessageFormat.format("Unknown field type encountered {0}", fieldType));
    }
    fieldVector.allocateNew();
    return fieldVector;
  }

  private void putValue(
      int index,
      FieldVector fieldVector,
      ClientTypeSignature fieldType,
      Object objectValue) throws Exception {

    if (objectValue == null) {
      if (fieldVector instanceof BaseFixedWidthVector) {
        ((BaseFixedWidthVector) fieldVector).setNull(index);
        return;
      } else if (fieldVector instanceof BaseVariableWidthVector) {
        ((BaseVariableWidthVector) fieldVector).setNull(index);
        return;
      } else {
        throw new Exception(
            MessageFormat.format(
                "Filed vector is not of type BaseFixedWidthVector or BaseVariableWidthVector and it is of type {0}",
                fieldVector.getClass().getName()));
      }
    }

    if (fieldType.getRawType().equals(ClientStandardTypes.VARCHAR)
            || fieldType.getRawType().equals(ClientStandardTypes.CHAR)
        || fieldType.getRawType().equals(ClientStandardTypes.TIMESTAMP)
        || fieldType.getRawType().equals(ClientStandardTypes.DATE)) {
      ((VarCharVector) fieldVector)
          .setSafe(index, (objectValue.toString()).getBytes(StandardCharsets.UTF_8));
    } else if (fieldType.getRawType().equals(ClientStandardTypes.INTEGER)) {
      Integer value = (int)objectValue;
      ((IntVector) fieldVector).setSafe(index, value);
    } else if (fieldType.getRawType().equals(ClientStandardTypes.TINYINT)) {
      ((TinyIntVector) fieldVector).setSafe(index, (byte)objectValue);
    } else if (fieldType.getRawType().equals(ClientStandardTypes.SMALLINT)) {
      Integer value = (int)objectValue;
      ((SmallIntVector) fieldVector).setSafe(index, value);
    } else if (fieldType.getRawType().equals(ClientStandardTypes.DECIMAL)) {
      BigDecimal value = new BigDecimal(objectValue.toString());
      value = value.setScale(MAX_SCALE);
      ((DecimalVector) fieldVector).setSafe(index, value);
    } else if (fieldType.getRawType().equals(ClientStandardTypes.REAL)) {
      Float value = (float)objectValue;
      ((Float4Vector) fieldVector).setSafe(index, value);
    } else if (fieldType.getRawType().equals(ClientStandardTypes.DOUBLE)) {
      Double value = (double)objectValue;
      ((Float8Vector) fieldVector).setSafe(index, value);
    } else if (fieldType.getRawType().equals(ClientStandardTypes.BIGINT)) {
      long value = (long)objectValue;
      ((BigIntVector) fieldVector).setSafe(index, value);
    } else if (fieldType.getRawType().equals(ClientStandardTypes.BOOLEAN)) {
      boolean value = (boolean)objectValue;
      ((BitVector) fieldVector).setSafe(index, value ? 1 : 0);
    } else {
      throw new Exception(
          MessageFormat.format("Unknown field type encountered {0}", fieldType));
    }
  }
}
