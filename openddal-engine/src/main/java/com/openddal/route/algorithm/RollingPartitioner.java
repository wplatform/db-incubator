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

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import com.openddal.route.rule.ObjectNode;
import com.openddal.route.rule.RuleEvaluateException;
import com.openddal.util.StringUtils;
import com.openddal.value.Value;
import com.openddal.value.ValueTimestamp;

/**
 * @author jorgie.li
 */
public class RollingPartitioner extends CommonPartitioner {

    private static final int NUMBER_TYPE = 0;
    private static final int YEARS_TYPE = 1;
    private static final int MONTHS_TYPE = 2;
    private static final int DAYS_TYPE = 3;

    private String startBy;
    private String rollingBy;

    private long startNumber;
    private long rollingNumber;
    private int rollingType;

    @Override
    public void initialize(ObjectNode[] tableNodes) {
        super.initialize(tableNodes);
        if (StringUtils.isNullOrEmpty(rollingBy)) {
            throw new IllegalArgumentException("rollingBy is require.");
        }
        if (!StringUtils.isNullOrEmpty(startBy)) {
            startBy = startBy.trim();
            try {
                if (StringUtils.isNumber(startBy)) {
                    startNumber = Long.parseLong(startBy);
                } else {
                    SimpleDateFormat sdf = new SimpleDateFormat();
                    sdf.applyPattern("yyyy-MM-dd");
                    Date date = sdf.parse(startBy);
                    startNumber = date.getTime();
                }
            } catch (Exception e) {
                throw new IllegalArgumentException("startBy must be number or date of yyyy-MM-dd");
            }
        }
        rollingBy = rollingBy.replaceAll("\\s", "");
        if (StringUtils.isNumber(rollingBy)) {
            rollingNumber = Long.parseLong(rollingBy);
            rollingType = NUMBER_TYPE;
        } else if ("day".equalsIgnoreCase(rollingBy)) {
            rollingType = DAYS_TYPE;
        } else if ("month".equalsIgnoreCase(rollingBy)) {
            rollingType = MONTHS_TYPE;
        } else if ("year".equalsIgnoreCase(rollingBy)) {
            rollingType = YEARS_TYPE;
        } else {
            throw new IllegalArgumentException("startBy must be number or string 'day','month','year'.");
        }

    }

    public void setRollingBy(String rollingBy) {
        this.rollingBy = rollingBy;
    }

    public void setStartBy(String startBy) {
        this.startBy = startBy;
    }

    @Override
    public Integer partition(Value value) {
        boolean isNull = checkNull(value);
        if (isNull) {
            return getDefaultNodeIndex();
        }
        switch (rollingType) {
            case NUMBER_TYPE: {
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
                        int position = (int) ((valueLong - startNumber) / rollingNumber);
                        return position;
                    default:
                        throw new RuleEvaluateException("Invalid type for " + getClass().getName());
                }
            }
            case DAYS_TYPE: {
                Calendar calendar = Calendar.getInstance();
                calendar.setTimeInMillis(getTime(value));
                return calendar.get(Calendar.DAY_OF_MONTH) - 1;
            }
            case MONTHS_TYPE: {
                Calendar calendar = Calendar.getInstance();
                calendar.setTimeInMillis(getTime(value));
                return calendar.get(Calendar.MONTH);
            }
            case YEARS_TYPE: {
                if (startNumber < 1) {
                    throw new IllegalArgumentException("need to setting startBy date.");
                }
                Calendar start = Calendar.getInstance();
                start.setTimeInMillis(startNumber);
                Calendar v = Calendar.getInstance();
                v.setTimeInMillis(getTime(value));
                int startYear = start.get(Calendar.YEAR);
                int year = v.get(Calendar.YEAR);
                return Math.max(year - startYear, 0);
            }
            default:
                throw new IllegalStateException("Invalid rollingType");
        }
    }


    @Override
    public Integer[] partition(Value beginValue, Value endValue) {
        Integer begin;
        if (beginValue == null) {
            begin = 0;
        } else {
            begin = partition(beginValue);
        }
        Integer end;
        if (endValue == null) {
            end = getTableNodes().length - 1;
        } else {
            end = partition(endValue);
        }

        int max = Math.max(begin, end);
        int min = Math.min(begin, end);
        Integer[] re = new Integer[(max - min) + 1];
        int idx = 0;
        for (Integer i = min; i <= max; i++) {
            re[idx++] = i;
        }
        return re;
    }

    private long getTime(Value value) {
        int type;
        type = value.getType();
        switch (type) {
            case Value.DATE:
            case Value.TIME:
            case Value.TIMESTAMP:
                ValueTimestamp v = (ValueTimestamp) value.convertTo(Value.TIMESTAMP);
                return v.getTimestamp().getTime();
            default:
                throw new RuleEvaluateException("Invalid type for " + getClass().getName());
        }
    }
}
