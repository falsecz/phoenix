package com.salesforce.phoenix.expression.aggregator;

import com.salesforce.phoenix.expression.Expression;
import com.salesforce.phoenix.expression.LiteralExpression;
import com.salesforce.phoenix.schema.ColumnModifier;
import com.salesforce.phoenix.schema.PDataType;
import com.salesforce.phoenix.schema.tuple.Tuple;
import com.salesforce.phoenix.util.ByteUtil;
import com.salesforce.phoenix.util.FirstAggregatorDataTransferWrappper;
import com.salesforce.phoenix.util.SizedUtil;
import java.util.List;
import java.util.TreeMap;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.WritableByteArrayComparable;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;

/**
 *
 * @author tzolkincz
 */
public class FirstAndLastServerAggregator extends BaseAggregator {

	protected final ImmutableBytesWritable value = new ImmutableBytesWritable(ByteUtil.EMPTY_BYTE_ARRAY);
	protected List<Expression> children;
	protected WritableByteArrayComparable topOrder = new BinaryComparator(ByteUtil.EMPTY_BYTE_ARRAY);
	protected byte[] topValue;
	protected boolean useOffset = false;
	protected int selectOffset = -1;
	protected TreeMap<byte[], byte[]> topValues;
	protected boolean isAscending;

	public FirstAndLastServerAggregator(ColumnModifier columnModifier) {
		super(columnModifier);

	}

	@Override
	public void reset() {
		value.set(ByteUtil.EMPTY_BYTE_ARRAY);
		topOrder = new BinaryComparator(ByteUtil.EMPTY_BYTE_ARRAY);
		topValue = null;

		//@TODO reset offset values
		super.reset();
	}

	@Override
	public int getSize() {
		return super.getSize() + SizedUtil.IMMUTABLE_BYTES_WRITABLE_SIZE;
	}

	@Override
	public void aggregate(Tuple tuple, ImmutableBytesWritable ptr) {

		//set pointer to ordering by field
		children.get(1).evaluate(tuple, ptr);

		byte[] currentOrder = ptr.copyBytes();


		//int currentOrder = children.get(1).getDataType().getCodec().decodeInt(ptr, children.get(1).getColumnModifier());

		if (useOffset) {
			/*
			 children.get(0).evaluate(tuple, ptr);

			 if (topValues.size() < selectOffset) {
			 topValues.put(currentOrder.getValue(), ptr.copyBytes());
			 } else {
			 byte[] lowestKey = topValues.firstKey();
			 if (currentOrder.compareTo(lowestKey) > 0) {
			 topValues.remove(lowestKey);
			 topValues.put(currentOrder.getValue(), ptr.copyBytes());
			 }
			 }
			 value.set(topValues.get(topValues.firstKey()));
			 */
		} else {

			boolean isHigher;
			if (isAscending) {
				isHigher = topOrder.compareTo(currentOrder) > 0;
			} else {
				isHigher = topOrder.compareTo(currentOrder) < 0;//desc
			}
			if (topOrder.getValue().length < 1 || isHigher) {

				//set pointer to value
				children.get(0).evaluate(tuple, ptr);

				topValue = ptr.copyBytes();
				topOrder = new BinaryComparator(currentOrder);
			}
		}

	}

	@Override
	public String toString() {
		return "FirstAndLastServerAggregator [value=" + Bytes.toStringBinary(value.get(), value.getOffset(), value.getLength()) + "]";
	}

	@Override
	public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {

		if (topValue == null) {
			return false;
		}

		FirstAggregatorDataTransferWrappper payload = new FirstAggregatorDataTransferWrappper();
		payload.setOrdertValue(topOrder.getValue());
		payload.setValue(topValue);
		payload.setIsAscending(isAscending);

		ptr.set(payload.getBytesMessage());
		return true;
	}

	@Override
	public PDataType getDataType() {
		return PDataType.VARBINARY;
	}

	public void init(List<Expression> children, boolean isAscending) {
		this.children = children;

		if (children.size() > 3 && children.get(3) instanceof LiteralExpression) {
			if (((LiteralExpression) children.get(3)).getValue() != null) {

				try {
					selectOffset = ((Number) ((LiteralExpression) children.get(4)).getValue()).intValue();
				} catch (Exception e) {
					throw new IllegalArgumentException("init offset parameter error - FirstAndLastServerAggreator");
				}
				if (selectOffset > 0) {
					useOffset = true;
					topValues = new TreeMap<byte[], byte[]>();
				}
			}
		}

		this.isAscending = isAscending;

	}
}
