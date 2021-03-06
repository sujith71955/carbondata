/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.carbondata.processing.csvreaderstep;

import java.io.IOException;
import java.io.InputStream;

import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.trans.step.BaseStepData;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.steps.textfileinput.EncodingType;

public class CsvInputData extends BaseStepData implements StepDataInterface {
    public RowMetaInterface convertRowMeta;
    public RowMetaInterface outputRowMeta;

    public byte[] byteBuffer;
    public int startBuffer;
    public int endBuffer;
    public int bufferSize;

    public byte[] delimiter;
    public byte[] enclosure;

    public int preferredBufferSize;
    public String[] filenames;
    public int filenr;
    public byte[] binaryFilename;
    public boolean isAddingRowNumber;
    public long rowNumber;
    public int totalNumberOfSteps;
    public long bytesToSkipInFirstFile;
    public long totalBytesRead;
    public boolean parallel;
    public int filenameFieldIndex;
    public int rownumFieldIndex;
    /**
     * <pre>
     * if true then when double enclosure appears one will be considered as escape enclosure
     * Ecample: 'abc''xyz' would be processed as abc'xyz
     * </pre>
     */
    public EncodingType encodingType;
    public PatternMatcherInterface delimiterMatcher;
    public PatternMatcherInterface enclosureMatcher;
    public CrLfMatcherInterface crLfMatcher;
    protected InputStream bufferedInputStream;

    /**
     *
     */
    public CsvInputData() {
        super();
        byteBuffer = new byte[] {};
    }

    // Resize
    public void resizeByteBufferArray() {
        // What's the new size?
        // It's (endBuffer-startBuffer)+size !!
        // That way we can at least read one full block of data using NIO
        //
        bufferSize = endBuffer - startBuffer;
        int newSize = bufferSize + preferredBufferSize;
        byte[] newByteBuffer = new byte[newSize + 100];

        // copy over the old data...
        System.arraycopy(byteBuffer, startBuffer, newByteBuffer, 0, bufferSize);

        // replace the old byte buffer...
        byteBuffer = newByteBuffer;

        // Adjust start and end point of data in the byte buffer
        //
        startBuffer = 0;
        endBuffer = bufferSize;
    }

    public int readBufferFromFile() throws IOException {
        // See if the line is not longer than the buffer.
        // In that case we need to increase the size of the byte buffer.
        // Since this method doesn't get called every other character,
        // I'm sure we can spend a bit of time here without major performance loss.
        //

        int read = bufferedInputStream.read(byteBuffer, endBuffer, (byteBuffer.length - endBuffer));
        if (read >= 0) {
            // adjust the highest used position...
            //
            bufferSize = endBuffer + read;
        }
        return read;
    }

    /**
     * Increase the endBuffer pointer by one.<br>
     * If there is not enough room in the buffer to go there, resize the byte buffer and read more
     * data.<br>
     * if there is no more data to read and if the endBuffer pointer has reached the end of the byte
     * buffer, we return true.<br>
     *
     * @return true if we reached the end of the byte buffer.
     * @throws IOException In case we get an error reading from the input file.
     */
    public boolean increaseEndBuffer() throws IOException {
        endBuffer++;

        if (endBuffer >= bufferSize) {
            // Oops, we need to read more data...
            // Better resize this before we read other things in it...
            //
            resizeByteBufferArray();

            // Also read another chunk of data, now that we have the space for it...
            //
            int n = readBufferFromFile();

            // Return true we didn't manage to read anything and we reached the end of the buffer...
            //
            return n < 0;
        }

        return false;
    }

    /**
     * <pre>
     * [abcd "" defg] --> [abcd " defg]
     * [""""] --> [""]
     * [""] --> ["]
     * </pre>
     *
     * @return the byte array with escaped enclosures escaped.
     */
    public byte[] removeEscapedEnclosures(byte[] field, int nrEnclosuresFound) {
        byte[] result = new byte[field.length - nrEnclosuresFound];
        int resultIndex = 0;
        for (int i = 0; i < field.length; i++) {
            if (field[i] == enclosure[0]) {
                if (!(i + 1 < field.length && field[i + 1] == enclosure[0])) {
                    // Not an escaped enclosure...
                    result[resultIndex++] = field[i];
                }
            } else {
                result[resultIndex++] = field[i];
            }
        }
        return result;
    }

}
