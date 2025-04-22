package io.trino.plugin.hudi.util;

import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.Type;

/**
 * Strategy interface for handling different types of synthesized columns
 */
public interface SynthesizedColumnStrategy {

    void appendToBlock(BlockBuilder blockBuilder, Type type);

}
