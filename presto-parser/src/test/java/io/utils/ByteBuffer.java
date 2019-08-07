package io.utils;

public class ByteBuffer {

    private byte[] buffer;
    private int capacity;
    private int offset = 0;

    public ByteBuffer() {
        this(1024);
    }

    public ByteBuffer(int capacity) {
        buffer = new byte[capacity];
        this.capacity = capacity;
    }

    public void append(byte[] end) {

        if (null == end) {
            throw new NullPointerException("not support null buffer to put!");
        }

        if (end.length > capacity - offset) {

            realloc(end.length + offset * 2 );
        }
        offset += end.length;
        System.arraycopy(end, 0, buffer, offset, end.length);
    }

    public void append(byte[] end, int from, int length) {

        if (null == end) {
            throw new NullPointerException("not support null buffer to put!");
        }

        if (from > end.length || length + from > end.length) {
            throw new IndexOutOfBoundsException();
        }

        if (length > capacity - offset) {

            realloc(end.length + offset * 2 );
        }
        System.arraycopy(end, from, buffer, offset, length);
        offset += length;
    }

    private void realloc(int newSize) {

        byte[] tmp = buffer;
        buffer = new byte[newSize];
        capacity = newSize;
        System.arraycopy(tmp, 0, buffer, 0, offset);

    }

    public int releaseData() {
        offset = 0;
        return capacity;
    }

    public int getCapacity() {
        return capacity;
    }

    public byte[] getData() {
        byte[] data = new byte[offset];
        System.arraycopy(buffer, 0, data, 0, offset);
        return data;
    }

    public int size() {
        return offset;
    }
}
