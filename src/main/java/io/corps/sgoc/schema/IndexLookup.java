package io.corps.sgoc.schema;

import com.google.common.base.Preconditions;
import com.google.protobuf.ByteString;
import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;

import java.io.IOException;
import java.util.Arrays;

import static com.google.protobuf.Descriptors.FieldDescriptor.JavaType;

/**
 * Created by corps@github.com on 2014/02/22.
 * Copyrighted by Zach Collins 2014
 */
public class IndexLookup {
  private final EntityIndex index;
  private final Object[] values;

  IndexLookup(EntityIndex index, Object[] values) {
    this.index = Preconditions.checkNotNull(index);
    this.values = Preconditions.checkNotNull(values);
    Preconditions.checkArgument(values.length == index.getFieldPaths().size());
  }

  @Override
  public int hashCode() {
    int result = index.hashCode();
    result = 31 * result + Arrays.hashCode(values);
    return result;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    IndexLookup that = (IndexLookup) o;

    if (!index.equals(that.index)) return false;
    // Probably incorrect - comparing Object[] arrays with Arrays.equals
    if (!Arrays.equals(values, that.values)) return false;

    return true;
  }

  @Override
  public String toString() {
    return String.format("IndexLookup: %s %s", index, Arrays.toString(values));
  }

  public byte[] binaryEncodeValues() throws IOException {
    int size = computerBinaryEncodingSize();
    byte[] result = new byte[size];
    encode(CodedOutputStream.newInstance(result));
    return result;
  }

  public Object[] getValues() {
    return values;
  }

  public EntityIndex getIndex() {
    return index;
  }

  private void encode(CodedOutputStream codedOutputStream) throws IOException {
    for (int i = 0; i < values.length; ++i) {
      int tag = i + 1;
      Object value = values[i];
      if (value == null) {
        continue;
      }

      JavaType javaType = index.getFieldPaths().get(i).getTerminatingType().getJavaType();

      switch (javaType) {
        case INT:
          codedOutputStream.writeFixed32(tag, (Integer) value);
          break;
        case LONG:
          codedOutputStream.writeFixed64(tag, (Long) value);
          break;
        case FLOAT:
          codedOutputStream.writeFloat(tag, (Float) value);
          break;
        case DOUBLE:
          codedOutputStream.writeDouble(tag, (Double) value);
          break;
        case BOOLEAN:
          codedOutputStream.writeBool(tag, (Boolean) value);
          break;
        case STRING:
          codedOutputStream.writeString(tag, (String) value);
          break;
        case BYTE_STRING:
          codedOutputStream.writeBytes(tag, (ByteString) value);
          break;
        case ENUM:
          codedOutputStream.writeEnum(tag, ((Descriptors.EnumValueDescriptor) value).getNumber());
          break;
        case MESSAGE:
          codedOutputStream.writeMessage(tag, (Message) value);
          break;
      }
    }
  }

  private int computerBinaryEncodingSize() {
    int size = 0;

    for (int i = 0; i < values.length; ++i) {
      Object value = values[i];
      if (value == null) {
        continue;
      }

      int tag = i + 1;
      JavaType javaType = index.getFieldPaths().get(i).getTerminatingType().getJavaType();

      switch (javaType) {
        case INT:
          size += CodedOutputStream.computeFixed32Size(tag, (Integer) value);
          break;
        case LONG:
          size += CodedOutputStream.computeFixed64Size(tag, (Long) value);
          break;
        case FLOAT:
          size += CodedOutputStream.computeFloatSize(tag, (Float) value);
          break;
        case DOUBLE:
          size += CodedOutputStream.computeDoubleSize(tag, (Double) value);
          break;
        case BOOLEAN:
          size += CodedOutputStream.computeBoolSize(tag, (Boolean) value);
          break;
        case STRING:
          size += CodedOutputStream.computeStringSize(tag, (String) value);
          break;
        case BYTE_STRING:
          size += CodedOutputStream.computeBytesSize(tag, (ByteString) value);
          break;
        case ENUM:
          size += CodedOutputStream.computeEnumSize(tag,
              ((Descriptors.EnumValueDescriptor) value).getNumber());
          break;
        case MESSAGE:
          size += CodedOutputStream.computeMessageSize(tag, (Message) value);
          break;
      }
    }

    return size;
  }

}
