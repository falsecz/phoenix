/*******************************************************************************
 * Copyright (c) 2013, Salesforce.com, Inc.
 * All rights reserved.
 * 
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 * 
 *     Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *     Redistributions in binary form must reproduce the above copyright notice,
 *     this list of conditions and the following disclaimer in the documentation
 *     and/or other materials provided with the distribution.
 *     Neither the name of Salesforce.com nor the names of its contributors may 
 *     be used to endorse or promote products derived from this software without 
 *     specific prior written permission.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE 
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL 
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR 
 * SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER 
 * CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, 
 * OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE 
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 ******************************************************************************/
package com.salesforce.phoenix.expression.aggregator;

import java.math.BigDecimal;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

import com.salesforce.phoenix.schema.ColumnModifier;
import com.salesforce.phoenix.schema.PDataType;
import com.salesforce.phoenix.schema.tuple.Tuple;
import com.salesforce.phoenix.util.SizedUtil;


/**
 * 
 * Aggregator that sums BigDecimal values
 *
 * @author jtaylor
 * @since 0.1
 */
public class DecimalSumAggregator extends BaseAggregator {
    private BigDecimal sum = BigDecimal.ZERO;
    private byte[] sumBuffer;
    
    public DecimalSumAggregator(ColumnModifier columnModifier, ImmutableBytesWritable ptr) {
        super(columnModifier);
        if (ptr != null) {
            initBuffer();
            sum = (BigDecimal)PDataType.DECIMAL.toObject(ptr);
        }
    }
    
    private PDataType getInputDataType() {
        return PDataType.DECIMAL;
    }
    
    private int getBufferLength() {
        return getDataType().getByteSize();
    }

    private void initBuffer() {
        sumBuffer = new byte[getBufferLength()];
    }
    
    @Override
    public void aggregate(Tuple tuple, ImmutableBytesWritable ptr) {
        BigDecimal value = (BigDecimal)getDataType().toObject(ptr, getInputDataType(), columnModifier);
        sum = sum.add(value);
        if (sumBuffer == null) {
            sumBuffer = new byte[getDataType().getByteSize()];
        }
    }
    
    @Override
    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
        if (sumBuffer == null) {
            return false;
        }
        int len = getDataType().toBytes(sum, sumBuffer, 0);
        ptr.set(sumBuffer, 0, len);
        return true;
    }
    
    @Override
    public final PDataType getDataType() {
        return PDataType.DECIMAL;
    }
    
    @Override
    public void reset() {
        sum = BigDecimal.ZERO;
        sumBuffer = null;
        super.reset();
    }

    @Override
    public String toString() {
        return "DECIMAL SUM [sum=" + sum + "]";
    }

    @Override
    public int getSize() {
        return super.getSize() + SizedUtil.BIG_DECIMAL_SIZE + SizedUtil.ARRAY_SIZE + getDataType().getByteSize();
    }
}
