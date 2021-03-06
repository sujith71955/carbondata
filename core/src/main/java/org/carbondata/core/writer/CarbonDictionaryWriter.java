package org.carbondata.core.writer;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

/**
 * dictionary writer interface
 */
public interface CarbonDictionaryWriter extends Closeable {
    /**
     * write method that accepts one value at a time
     * This method can be used when data is huge and memory is les. In that
     * case data can be stored to a file and an iterator can iterate over it and
     * pass one value at a time
     */
    void write(String value) throws IOException;

    /**
     * write method that accepts list of byte arrays as value
     * This can be used when data is less, then string can be converted
     * to byte array for each value and added to a list
     */
    void write(List<byte[]> valueList) throws IOException;
}
