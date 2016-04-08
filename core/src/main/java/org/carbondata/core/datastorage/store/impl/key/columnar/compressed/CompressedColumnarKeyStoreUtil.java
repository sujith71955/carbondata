package org.carbondata.core.datastorage.store.impl.key.columnar.compressed;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.carbondata.core.datastorage.store.columnar.ColumnarKeyStoreDataHolder;
import org.carbondata.core.datastorage.store.columnar.ColumnarKeyStoreInfo;
import org.carbondata.core.datastorage.store.columnar.ColumnarKeyStoreMetadata;

/**
 * Utility helper class for managing the processing of columnar key store block.
 * 
 * @author s71955
 *
 */
public final class CompressedColumnarKeyStoreUtil {

    private CompressedColumnarKeyStoreUtil() {

    }
	/**
	 * @author s71955 The high cardinality dimensions rows will be send in byte
	 *         array with its data length appended in the
	 *         ColumnarKeyStoreDataHolder byte array since high cardinality dim
	 *         data will not be part of MDKey/Surrogate keys. In this method the
	 *         byte array will be scanned and the length which is stored in
	 *         short will be removed.
	 * @param columnarKeyBlockData
	 * @param columnarKeyStoreMetadata
	 * @return
	 */
	public static List<byte[]> readColumnarKeyBlockDataForNoDictionaryCols(
			byte[] columnarKeyBlockData) {
		List<byte[]> columnarKeyBlockDataList = new ArrayList<byte[]>(50);
		ByteBuffer directSurrogateKeyStoreDataHolder = ByteBuffer
				.allocate(columnarKeyBlockData.length);
		directSurrogateKeyStoreDataHolder.put(columnarKeyBlockData);
		directSurrogateKeyStoreDataHolder.flip();
		while (directSurrogateKeyStoreDataHolder.hasRemaining()) {
			short dataLength = directSurrogateKeyStoreDataHolder.getShort();
			byte[] directSurrKeyData = new byte[dataLength];
			directSurrogateKeyStoreDataHolder.get(directSurrKeyData);
			columnarKeyBlockDataList.add(directSurrKeyData);
		}
		return columnarKeyBlockDataList;

	}

	/**
	 * 
	 * @param blockIndex
	 * @param columnarKeyBlockData
	 * @param columnKeyBlockIndex
	 * @param columnKeyBlockReverseIndex
	 * @param columnarStoreInfo
	 * @return
	 */
	public static ColumnarKeyStoreDataHolder createColumnarKeyStoreMetadataForHCDims(
			int blockIndex, byte[] columnarKeyBlockData,
			int[] columnKeyBlockIndex, int[] columnKeyBlockReverseIndex,
			ColumnarKeyStoreInfo columnarStoreInfo) {
		ColumnarKeyStoreMetadata columnarKeyStoreMetadata;
		columnarKeyStoreMetadata = new ColumnarKeyStoreMetadata(0);
		columnarKeyStoreMetadata.setDirectSurrogateColumn(true);
		columnarKeyStoreMetadata.setColumnIndex(columnKeyBlockIndex);
		columnarKeyStoreMetadata
				.setColumnReverseIndex(columnKeyBlockReverseIndex);
		columnarKeyStoreMetadata
				.setSorted(columnarStoreInfo.getIsSorted()[blockIndex]);
		columnarKeyStoreMetadata.setUnCompressed(true);
		List<byte[]> directSurrogateBasedKeyBlockData = CompressedColumnarKeyStoreUtil
				.readColumnarKeyBlockDataForNoDictionaryCols(columnarKeyBlockData);
		ColumnarKeyStoreDataHolder columnarKeyStoreDataHolders = new ColumnarKeyStoreDataHolder(
				directSurrogateBasedKeyBlockData, columnarKeyStoreMetadata);
		return columnarKeyStoreDataHolders;
	}

    /**
     * This API will determine whether the requested block index is a  No dictionary
     * column index.
     * @param directSurrogates
     * @param blockIndex
     * @return
     */
    public static boolean isHighCardinalityBlock(int[] directSurrogates, int blockIndex) {
        if (null != directSurrogates) {
            for (int directSurrogateIndex : directSurrogates) {
                if (directSurrogateIndex == blockIndex) {
                    return true;
                }
            }
        }
        return false;
    }
}
