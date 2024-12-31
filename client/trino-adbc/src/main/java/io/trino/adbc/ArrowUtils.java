package io.trino.adbc;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;

public class ArrowUtils {
    private static BufferAllocator rootAllocator = new RootAllocator();
    public static BufferAllocator root() {
        return rootAllocator;
    }
}
