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

package org.carbondata.query.evaluators;

import java.util.List;
import java.util.Map;

import org.carbondata.core.carbon.metadata.schema.table.column.CarbonDimension;
import org.carbondata.core.metadata.CarbonMetadata.Dimension;
import org.carbondata.query.complex.querytypes.GenericQueryType;
import org.carbondata.query.datastorage.InMemoryTable;

public class DimColumnEvaluatorInfo {
    /**
     * column index in file
     */
    private int columnIndex = -1;

    /**
     * need compressed data from file
     */
    private boolean needCompressedData;

    /**
     * list of filter need to apply
     */
    private byte[][] filterValues;

    /**
     * slice
     */
    private List<InMemoryTable> slices;

    /**
     * currentSliceIndex
     */
    private int currentSliceIndex;

    /**
     * dims
     */
    private Dimension dims;

    private Dimension[] dimensions;

    /**
     * rowIndex
     */
    private int rowIndex = -1;

    private boolean isDimensionExistsInCurrentSilce = true;

    private int rsSurrogates;

    private String defaultValue;

    private Map<Integer, GenericQueryType> complexTypesWithBlockStartIndex;
    
	private CarbonDimension dimension;

    public Map<Integer, GenericQueryType> getComplexTypesWithBlockStartIndex() {
        return complexTypesWithBlockStartIndex;
    }

    public void setComplexTypesWithBlockStartIndex(
            Map<Integer, GenericQueryType> complexTypesWithBlockStartIndex) {
        this.complexTypesWithBlockStartIndex = complexTypesWithBlockStartIndex;
    }
    
    public CarbonDimension getDimension() {
		return dimension;
	}

	public void setDimension(CarbonDimension dimension) {
		this.dimension = dimension;
	}

    public Dimension[] getDimensions() {
        return dimensions;
    }

    public void setDimensions(Dimension[] dimensions) {
        this.dimensions = dimensions;
    }

    public int getColumnIndex() {
        return columnIndex;
    }

    public void setColumnIndex(int columnIndex) {
        this.columnIndex = columnIndex;
    }

    public boolean isNeedCompressedData() {
        return needCompressedData;
    }

    public void setNeedCompressedData(boolean needCompressedData) {
        this.needCompressedData = needCompressedData;
    }

    public byte[][] getFilterValues() {
        return filterValues;
    }

    public void setFilterValues(final byte[][] filterValues) {
        this.filterValues = filterValues;
    }

    public int getRowIndex() {
        return rowIndex;
    }

    public void setRowIndex(int rowIndex) {
        this.rowIndex = rowIndex;
    }

    public Dimension getDims() {
        return dims;
    }

    public void setDims(Dimension dims) {
        this.dims = dims;
    }

    public List<InMemoryTable> getSlices() {
        return slices;
    }

    public void setSlices(List<InMemoryTable> slices) {
        this.slices = slices;
    }

    public int getCurrentSliceIndex() {
        return currentSliceIndex;
    }

    public void setCurrentSliceIndex(int currentSliceIndex) {
        this.currentSliceIndex = currentSliceIndex;
    }

    public boolean isDimensionExistsInCurrentSilce() {
        return isDimensionExistsInCurrentSilce;
    }

    public void setDimensionExistsInCurrentSilce(boolean isDimensionExistsInCurrentSilce) {
        this.isDimensionExistsInCurrentSilce = isDimensionExistsInCurrentSilce;
    }

    public int getRsSurrogates() {
        return rsSurrogates;
    }

    public void setRsSurrogates(int rsSurrogates) {
        this.rsSurrogates = rsSurrogates;
    }

    public String getDefaultValue() {
        return defaultValue;
    }

    public void setDefaultValue(String defaultValue) {
        this.defaultValue = defaultValue;
    }
}
