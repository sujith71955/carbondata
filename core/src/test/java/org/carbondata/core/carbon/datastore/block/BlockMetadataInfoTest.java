package org.carbondata.core.carbon.datastore.block;

import java.util.*;
import java.util.Map.Entry;

import junit.framework.TestCase;
import org.carbondata.core.carbon.metadata.datatype.DataType;
import org.carbondata.core.carbon.metadata.encoder.Encoding;
import org.carbondata.core.carbon.metadata.schema.table.column.ColumnSchema;
import org.carbondata.core.constants.CarbonCommonConstants;
import org.junit.BeforeClass;
import org.junit.Test;

public class BlockMetadataInfoTest extends TestCase {

    private SegmentProperties blockMetadataInfos;

    @BeforeClass public void setUp() {
        List<ColumnSchema> columnSchema = new ArrayList<ColumnSchema>();
        columnSchema.add(getDimensionColumn1());
        columnSchema.add(getDimensionColumn2());
        columnSchema.add(getDimensionColumn3());
        columnSchema.add(getDimensionColumn4());
        columnSchema.add(getDimensionColumn5());
        columnSchema.add(getDimensionColumn9());
        columnSchema.add(getDimensionColumn10());
        columnSchema.add(getDimensionColumn11());
        columnSchema.add(getDimensionColumn6());
        columnSchema.add(getDimensionColumn7());
        columnSchema.add(getMeasureColumn());
        columnSchema.add(getMeasureColumn1());
        int[] cardinality = new int[columnSchema.size()];
        int x = 100;
        for (int i = 0; i < columnSchema.size(); i++) {
            cardinality[i] = x;
            x++;
        }
        blockMetadataInfos = new SegmentProperties(columnSchema, cardinality);
    }

    @Test public void testBlockMetadataHasProperDimensionCardinality() {
        int[] cardinality = { 100, 102, 103, 105, 106, 107 };
        boolean isProper = true;
        for (int i = 0; i < cardinality.length; i++) {
            isProper = cardinality[i] == blockMetadataInfos.getDimColumnsCardinality()[i];
            if (!isProper) {
                assertTrue(false);
            }
        }
        assertTrue(true);
    }

    @Test public void testBlockMetadataHasProperComplesDimensionCardinality() {
        int[] cardinality = { 108, 109 };
        boolean isProper = true;
        for (int i = 0; i < cardinality.length; i++) {
            isProper = cardinality[i] == blockMetadataInfos.getComplexDimColumnCardinality()[i];
            if (!isProper) {
                assertTrue(false);
            }
        }
        assertTrue(true);
    }

    @Test public void testBlockMetadataHasProperDimensionBlockMapping() {
        Map<Integer, Integer> dimensionOrdinalToBlockMapping = new HashMap<Integer, Integer>();
        dimensionOrdinalToBlockMapping.put(0, 0);
        dimensionOrdinalToBlockMapping.put(1, 1);
        dimensionOrdinalToBlockMapping.put(2, 2);
        dimensionOrdinalToBlockMapping.put(3, 2);
        dimensionOrdinalToBlockMapping.put(4, 3);
        dimensionOrdinalToBlockMapping.put(5, 4);
        dimensionOrdinalToBlockMapping.put(6, 4);
        dimensionOrdinalToBlockMapping.put(7, 4);
        dimensionOrdinalToBlockMapping.put(8, 5);
        dimensionOrdinalToBlockMapping.put(9, 6);
        Map<Integer, Integer> dimensionOrdinalToBlockMappingActual =
                blockMetadataInfos.getDimensionOrdinalToBlockMapping();
        assertEquals(dimensionOrdinalToBlockMapping.size(),
                dimensionOrdinalToBlockMappingActual.size());
        Iterator<Entry<Integer, Integer>> iterator =
                dimensionOrdinalToBlockMapping.entrySet().iterator();
        while (iterator.hasNext()) {
            Entry<Integer, Integer> next = iterator.next();
            Integer integer = dimensionOrdinalToBlockMappingActual.get(next.getKey());
            if (integer != next.getValue()) {
                assertTrue(false);
            }
        }
        assertTrue(true);
    }

    @Test public void testBlockMetadataHasProperMeasureBlockMapping() {
        Map<Integer, Integer> measureOrdinalToBlockMapping = new HashMap<Integer, Integer>();
        measureOrdinalToBlockMapping.put(0, 0);
        measureOrdinalToBlockMapping.put(1, 1);
        Map<Integer, Integer> measureOrdinalToBlockMappingActual =
                blockMetadataInfos.getMeasuresOrdinalToBlockMapping();
        assertEquals(measureOrdinalToBlockMapping.size(),
                measureOrdinalToBlockMappingActual.size());
        Iterator<Entry<Integer, Integer>> iterator =
                measureOrdinalToBlockMapping.entrySet().iterator();
        while (iterator.hasNext()) {
            Entry<Integer, Integer> next = iterator.next();
            Integer integer = measureOrdinalToBlockMappingActual.get(next.getKey());
            if (integer != next.getValue()) {
                assertTrue(false);
            }
        }
        assertTrue(true);
    }

    @Test public void testNumberOfDimensionsIsCorrect() {
        assertEquals(8, blockMetadataInfos.getDimensions().size());
    }

    @Test public void testNumberOfMeasuesIsCorrect() {
        assertEquals(2, blockMetadataInfos.getMeasures().size());
    }

    @Test public void testNumberOfComplexDimensionIsCorrect() {
        assertEquals(2, blockMetadataInfos.getComplexDimensions().size());
    }

    @Test public void testRowGroupToCardinalityMappingHasProperValue() {
        Map<Integer, int[]> rowGroupAndItsCardinalityMapping =
                blockMetadataInfos.getRowGroupAndItsCardinalityMapping();
        assertEquals(rowGroupAndItsCardinalityMapping.get(0).length, 2);
        assertEquals(rowGroupAndItsCardinalityMapping.get(1).length, 3);
    }

    @Test public void testEachColumnValueSizeHasProperValue() {
        int[] size = { 1, -1, 2, -1, 3 };
        int[] eachDimColumnValueSize = blockMetadataInfos.getEachDimColumnValueSize();
        boolean isEqual = false;
        for (int i = 0; i < size.length; i++) {
            isEqual = size[i] == eachDimColumnValueSize[i];
            if (!isEqual) {
                assertTrue(false);
            }
        }
        assertTrue(true);
    }

    @Test public void testEachComplexColumnValueSizeHasProperValue() {
        int[] size = { 8, 8 };
        int[] eachDimColumnValueSize = blockMetadataInfos.getEachComplexDimColumnValueSize();
        boolean isEqual = false;
        for (int i = 0; i < size.length; i++) {
            isEqual = size[i] == eachDimColumnValueSize[i];
            if (!isEqual) {
                assertTrue(false);
            }
        }
        assertTrue(true);
    }

    private ColumnSchema getDimensionColumn1() {
        ColumnSchema dimColumn = new ColumnSchema();
        dimColumn.setColumnar(true);
        dimColumn.setColumnName("IMEI");
        dimColumn.setColumnUniqueId(1);
        dimColumn.setDataType(DataType.STRING);
        dimColumn.setDimensionColumn(true);
        Set<Encoding> encodeList =
                new HashSet<Encoding>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
        encodeList.add(Encoding.DICTIONARY);
        dimColumn.setEncodintList(encodeList);
        dimColumn.setNumberOfChild(0);
        return dimColumn;
    }

    private ColumnSchema getDimensionColumn2() {
        ColumnSchema dimColumn = new ColumnSchema();
        dimColumn.setColumnar(true);
        dimColumn.setColumnName("IMEI1");
        dimColumn.setColumnUniqueId(1);
        dimColumn.setDataType(DataType.STRING);
        dimColumn.setDimensionColumn(true);
        Set<Encoding> encodeList =
                new HashSet<Encoding>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
        dimColumn.setEncodintList(encodeList);
        dimColumn.setNumberOfChild(0);
        return dimColumn;
    }

    private ColumnSchema getDimensionColumn3() {
        ColumnSchema dimColumn = new ColumnSchema();
        dimColumn.setColumnar(false);
        dimColumn.setColumnName("IMEI2");
        dimColumn.setColumnUniqueId(1);
        dimColumn.setDataType(DataType.STRING);
        dimColumn.setDimensionColumn(true);
        Set<Encoding> encodeList =
                new HashSet<Encoding>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
        encodeList.add(Encoding.DICTIONARY);
        dimColumn.setEncodintList(encodeList);
        dimColumn.setRowGroupId(0);
        dimColumn.setNumberOfChild(0);
        return dimColumn;
    }

    private ColumnSchema getDimensionColumn4() {
        ColumnSchema dimColumn = new ColumnSchema();
        dimColumn.setColumnar(false);
        dimColumn.setColumnName("IMEI3");
        dimColumn.setColumnUniqueId(1);
        dimColumn.setDataType(DataType.STRING);
        dimColumn.setDimensionColumn(true);
        Set<Encoding> encodeList =
                new HashSet<Encoding>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
        encodeList.add(Encoding.DICTIONARY);
        dimColumn.setEncodintList(encodeList);
        dimColumn.setNumberOfChild(0);
        dimColumn.setRowGroupId(0);
        return dimColumn;
    }

    private ColumnSchema getDimensionColumn5() {
        ColumnSchema dimColumn = new ColumnSchema();
        dimColumn.setColumnar(true);
        dimColumn.setColumnName("IMEI4");
        dimColumn.setColumnUniqueId(1);
        dimColumn.setDataType(DataType.STRING);
        dimColumn.setDimensionColumn(true);
        Set<Encoding> encodeList =
                new HashSet<Encoding>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
        dimColumn.setEncodintList(encodeList);
        dimColumn.setNumberOfChild(0);
        return dimColumn;
    }

    private ColumnSchema getDimensionColumn9() {
        ColumnSchema dimColumn = new ColumnSchema();
        dimColumn.setColumnar(false);
        dimColumn.setColumnName("IMEI9");
        dimColumn.setColumnUniqueId(1);
        dimColumn.setDataType(DataType.STRING);
        dimColumn.setDimensionColumn(true);
        Set<Encoding> encodeList =
                new HashSet<Encoding>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
        encodeList.add(Encoding.DICTIONARY);
        dimColumn.setEncodintList(encodeList);
        dimColumn.setRowGroupId(1);
        dimColumn.setNumberOfChild(0);
        return dimColumn;
    }

    private ColumnSchema getDimensionColumn10() {
        ColumnSchema dimColumn = new ColumnSchema();
        dimColumn.setColumnar(false);
        dimColumn.setColumnName("IMEI10");
        dimColumn.setColumnUniqueId(1);
        dimColumn.setDataType(DataType.STRING);
        dimColumn.setDimensionColumn(true);
        Set<Encoding> encodeList =
                new HashSet<Encoding>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
        encodeList.add(Encoding.DICTIONARY);
        dimColumn.setEncodintList(encodeList);
        dimColumn.setNumberOfChild(0);
        dimColumn.setRowGroupId(1);
        return dimColumn;
    }

    private ColumnSchema getDimensionColumn11() {
        ColumnSchema dimColumn = new ColumnSchema();
        dimColumn.setColumnar(false);
        dimColumn.setColumnName("IMEI11");
        dimColumn.setColumnUniqueId(1);
        dimColumn.setDataType(DataType.STRING);
        dimColumn.setDimensionColumn(true);
        Set<Encoding> encodeList =
                new HashSet<Encoding>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
        encodeList.add(Encoding.DICTIONARY);
        dimColumn.setEncodintList(encodeList);
        dimColumn.setNumberOfChild(0);
        dimColumn.setRowGroupId(1);
        return dimColumn;
    }

    private ColumnSchema getDimensionColumn6() {
        ColumnSchema dimColumn = new ColumnSchema();
        dimColumn.setColumnar(true);
        dimColumn.setColumnName("IMEI5");
        dimColumn.setColumnUniqueId(1);
        dimColumn.setDataType(DataType.ARRAY);
        dimColumn.setDimensionColumn(true);
        Set<Encoding> encodeList =
                new HashSet<Encoding>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
        encodeList.add(Encoding.DICTIONARY);
        dimColumn.setEncodintList(encodeList);
        dimColumn.setNumberOfChild(1);
        return dimColumn;
    }

    private ColumnSchema getDimensionColumn7() {
        ColumnSchema dimColumn = new ColumnSchema();
        dimColumn.setColumnar(true);
        dimColumn.setColumnName("IMEI6");
        dimColumn.setColumnUniqueId(1);
        dimColumn.setDataType(DataType.STRING);
        dimColumn.setDimensionColumn(true);
        Set<Encoding> encodeList =
                new HashSet<Encoding>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
        encodeList.add(Encoding.DICTIONARY);
        dimColumn.setEncodintList(encodeList);
        dimColumn.setNumberOfChild(0);
        return dimColumn;
    }

    private ColumnSchema getMeasureColumn() {
        ColumnSchema dimColumn = new ColumnSchema();
        dimColumn.setAggregateFunction("SUM");
        dimColumn.setColumnName("IMEI_COUNT");
        dimColumn.setColumnUniqueId(1);
        dimColumn.setDataType(DataType.STRING);
        Set<Encoding> encodeList =
                new HashSet<Encoding>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
        encodeList.add(Encoding.DELTA);
        dimColumn.setEncodintList(encodeList);
        return dimColumn;
    }

    private ColumnSchema getMeasureColumn1() {
        ColumnSchema dimColumn = new ColumnSchema();
        dimColumn.setAggregateFunction("SUM");
        dimColumn.setColumnName("IMEI_COUNT1");
        dimColumn.setColumnUniqueId(1);
        dimColumn.setDataType(DataType.STRING);
        Set<Encoding> encodeList =
                new HashSet<Encoding>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
        encodeList.add(Encoding.DELTA);
        dimColumn.setEncodintList(encodeList);
        return dimColumn;
    }

}
