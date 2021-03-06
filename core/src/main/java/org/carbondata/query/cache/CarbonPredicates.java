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

package org.carbondata.query.cache;

import java.io.Serializable;
import java.util.Arrays;

public class CarbonPredicates implements Serializable {

    private static final long serialVersionUID = -795961936216399659L;

    /**
     * include filters
     */
    private long[] include;

    /**
     * exclude filters
     */
    private long[] exclude;

    /**
     * include or filters
     */
    private long[] includeOr;

    /**
     * Ordinal
     */
    private int ordinal;

    public CarbonPredicates() {
        include = new long[0];
        exclude = new long[0];
        includeOr = new long[0];
    }

    /**
     * @return the include
     */
    public long[] getInclude() {
        return include;
    }

    /**
     * @param include the include to set
     */
    public void setInclude(long[] include) {
        Arrays.sort(include);
        this.include = include;
    }

    /**
     * @return the exclude
     */
    public long[] getExclude() {
        return exclude;
    }

    /**
     * @param exclude the exclude to set
     */
    public void setExclude(long[] exclude) {
        Arrays.sort(exclude);
        this.exclude = exclude;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + Arrays.hashCode(exclude);
        result = prime * result + Arrays.hashCode(include);
        result = prime * result + Arrays.hashCode(includeOr);
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof CarbonPredicates) {
            if (this == obj) {
                return true;
            }

            CarbonPredicates other = (CarbonPredicates) obj;
            if (!Arrays.equals(exclude, other.exclude)) {
                return false;
            }
            if (!Arrays.equals(include, other.include)) {
                return false;
            }
            if (!Arrays.equals(includeOr, other.includeOr)) {
                return false;
            }
            if (ordinal != other.ordinal) {
                return false;
            }
            return true;

        }

        return false;
    }

    /**
     * @return the ordinal
     */
    public int getOrdinal() {
        return ordinal;
    }

    /**
     * @param ordinal the ordinal to set
     */
    public void setOrdinal(int ordinal) {
        this.ordinal = ordinal;
    }

    /**
     * @return the includeOr
     */
    public long[] getIncludeOr() {
        return includeOr;
    }

    /**
     * @param includeOr the includeOr to set
     */
    public void setIncludeOr(long[] includeOr) {
        this.includeOr = includeOr;
    }

}
