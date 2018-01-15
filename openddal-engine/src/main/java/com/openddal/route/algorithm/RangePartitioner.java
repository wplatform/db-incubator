/*
 * Copyright 2014-2016 the original author or authors
 *
 * Licensed under the Apache License, Version 2.0 (the “License”);
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an “AS IS” BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.openddal.route.algorithm;

import com.openddal.route.rule.ObjectNode;
import com.openddal.route.rule.RuleEvaluateException;
import com.openddal.value.Value;
import com.openddal.value.ValueNull;
import com.openddal.value.ValueTimestamp;

/**
 * @author jorgie.li
 */
public class RangePartitioner extends CommonPartitioner {

    private int chunk = 1024;
    private int[] count;
    private int[] length;
    private PartitionUtil partitionUtil;

    /**
     * 字符串hash算法：s[0]*31^(n-1) + s[1]*31^(n-2) + ... + s[n-1] <br>
     * 其中s[]为字符串的字符数组，换算成程序的表达式为：<br>
     * h = 31*h + s.charAt(i); => h = (h << 5) - h + s.charAt(i); <br>
     *
     * @param start hash for s.substring(start, end)
     * @param end   hash for s.substring(start, end)
     */
    private static long hash(String s, int start, int end) {
        if (start < 0) {
            start = 0;
        }
        if (end > s.length()) {
            end = s.length();
        }
        long h = 0;
        for (int i = start; i < end; ++i) {
            h = (h << 5) - h + s.charAt(i);
        }
        return h;
    }

    @Override
    public void initialize(ObjectNode[] tableNodes) {
        super.initialize(tableNodes);
        if ((chunk & chunk - 1) != 0) {
            throw new IllegalArgumentException("Chunk must be 2^n,such as 256,512,1024...");
        }
        partitionUtil = new PartitionUtil(chunk, count, length);
    }

    public void setChunk(int chunk) {
        this.chunk = chunk;
    }

    public void setPartitionCount(String partitionCount) {
        this.count = toIntArray(partitionCount);
    }

    public void setPartitionLength(String partitionLength) {
        this.length = toIntArray(partitionLength);
    }

    @Override
    public Integer partition(Value value) {
        boolean isNull = checkNull(value);
        if (isNull) {
            return getDefaultNodeIndex();
        }
        int type = value.getType();
        switch (type) {
            case Value.BYTE:
            case Value.SHORT:
            case Value.INT:
            case Value.LONG:
            case Value.FLOAT:
            case Value.DECIMAL:
            case Value.DOUBLE:
                long valueLong = value.getLong();
                return partitionUtil.partition(valueLong);
            case Value.DATE:
            case Value.TIME:
            case Value.TIMESTAMP:
                ValueTimestamp v = (ValueTimestamp) value.convertTo(Value.TIMESTAMP);
                long toLong = v.getTimestamp().getTime();
                return partitionUtil.partition(toLong);
            case Value.STRING:
            case Value.STRING_FIXED:
            case Value.STRING_IGNORECASE:
                String string = value.getString();
                long hash = hash(string, 0, string.length());
                return partitionUtil.partition(hash);
            default:
                throw new RuleEvaluateException("Invalid type for " + getClass().getName());
        }
    }

    @Override
    public Integer[] partition(Value beginValue, Value endValue) {
        if (beginValue == null || beginValue == ValueNull.INSTANCE
                || endValue == null || endValue == ValueNull.INSTANCE) {
            return allNodes();
        }
        if (beginValue.getType() != endValue.getType()) {
            throw new RuleEvaluateException("Type is not consistent");
        }
        long vBegin;
        long vEnd;
        int type = beginValue.getType();
        switch (type) {
            case Value.BYTE:
            case Value.SHORT:
            case Value.INT:
            case Value.LONG:
                vBegin = beginValue.getLong();
                vEnd = endValue.getLong();
                break;
            case Value.FLOAT:
            case Value.DECIMAL:
            case Value.DOUBLE:
                double aDouble = beginValue.getDouble();
                double bDouble = endValue.getDouble();
                vBegin = Math.round(aDouble);
                vEnd = Math.round(bDouble);
                break;
            case Value.DATE:
            case Value.TIME:
            case Value.TIMESTAMP:
                vBegin = beginValue.getLong();
                vEnd = endValue.getLong();
                break;
            case Value.STRING:
            case Value.STRING_FIXED:
            case Value.STRING_IGNORECASE:
                String str1 = beginValue.getString();
                String str2 = endValue.getString();
                vBegin = hash(str1, 0, str1.length());
                vEnd = hash(str2, 0, str2.length());
                break;
            default:
                throw new RuleEvaluateException("Invalid type for " + getClass().getName());

        }
        if ((vEnd - vBegin) >= chunk - 1) {
            return allNodes();
        } else if ((vEnd - vBegin) < 0) {
            return new Integer[0];
        } else {
            Integer begin = partition(beginValue);
            Integer end = partition(endValue);
            int max = Math.max(begin, end);
            int min = Math.min(begin, end);
            Integer[] re = new Integer[(max - min) + 1];
            int idx = 0;
            for (Integer i = min; i <= max; i++) {
                re[idx++] = i;
            }
            return re;
        }
    }

}
