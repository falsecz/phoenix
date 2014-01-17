package com.salesforce.phoenix.expression.aggregator;

import com.salesforce.phoenix.expression.Expression;
import com.salesforce.phoenix.schema.ColumnModifier;
import com.salesforce.phoenix.schema.PDataType;
import com.salesforce.phoenix.schema.tuple.Tuple;
import com.salesforce.phoenix.util.BinarySerializableComparator;
import com.salesforce.phoenix.util.ByteUtil;
import com.salesforce.phoenix.util.FirstAggregatorDataTransferWrappper;
import com.salesforce.phoenix.util.FirstByLastByWithOffsetWrapper;
import com.salesforce.phoenix.util.SizedUtil;
import java.io.IOException;
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

	protected List<Expression> children;
	protected WritableByteArrayComparable topOrder = new BinaryComparator(ByteUtil.EMPTY_BYTE_ARRAY);
	protected byte[] topValue;
	protected boolean useOffset = false;
	protected int offset = -1;
	protected TreeMap<byte[], byte[]> topValues = new TreeMap<byte[], byte[]>(new BinarySerializableComparator());
	protected boolean isAscending;

	public FirstAndLastServerAggregator(ColumnModifier columnModifier) {
		super(columnModifier);

	}

	@Override
	public void reset() {
		topOrder = new BinaryComparator(ByteUtil.EMPTY_BYTE_ARRAY);
		topValue = null;
		topValues.clear();
		offset = -1;
		useOffset = false;

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

		if (useOffset) {
			children.get(0).evaluate(tuple, ptr);

			if (topValues.size() < offset) {
				try {
					children.get(0).evaluate(tuple, ptr);
					topValues.put(currentOrder, ptr.copyBytes());
				} catch (Exception e) {
					System.out.println("sracky sracky sracky");
					System.out.println(e.getCause());
					System.out.println(e.getMessage());
					System.out.println(e);
					e.printStackTrace();
				}
			} else {
				boolean add = false;
				if (isAscending) {
					byte[] lowestKey = topValues.lastKey();
					if (Bytes.compareTo(currentOrder, lowestKey) < 0) {
						topValues.remove(lowestKey);
						add = true;
					}
				} else { //desc
					byte[] hiestKey = topValues.firstKey();
					if (Bytes.compareTo(currentOrder, hiestKey) > 0) {
						topValues.remove(hiestKey);
						add = true;
					}
				}

				if (add) {
					children.get(0).evaluate(tuple, ptr);
					topValues.put(currentOrder, ptr.copyBytes());
				}
			}

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
		StringBuilder out = new StringBuilder("FirstAndLastServerAggregator"
				+ " is ascending: " + isAscending + " value=");
		if (useOffset) {
			for (byte[] key : topValues.keySet()) {
				out.append(topValues.get(key));
			}
			out.append(" offset = ").append(offset);
		} else {
			out.append(topValue);
		}

		return out.toString();
	}

	@Override
	public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {

		if (useOffset) {
			if (topValues.size() == 0) {
				return false;
			}

			FirstByLastByWithOffsetWrapper payload = new FirstByLastByWithOffsetWrapper();
			payload.setIsAscending(isAscending);
			payload.setOffset(offset);
			payload.setData(topValues);

			try {
				ptr.set(payload.getPayload());
			} catch (IOException ex) {
				System.out.println("jebka3: "+ex.getCause());
				ex.printStackTrace();
				System.out.println(ex);
				//cant throw nothing here - @TODO rly?
				return false;
			}
			return true;
		}

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

	public void init(List<Expression> children, boolean isAscending, int offset) {
		this.children = children;
		this.offset = offset;
		if (offset > 0) {
			useOffset = true;
		}

//		if (children.size() > 3 && children.get(3) instanceof LiteralExpression) {
//			if (((LiteralExpression) children.get(3)).getValue() != null) {
//
//				try {
//					selectOffset = ((Number) ((LiteralExpression) children.get(4)).getValue()).intValue();
//				} catch (Exception e) {
//					throw new IllegalArgumentException("init offset parameter error - SERVER aggregator");
//				}
//				if (selectOffset > 0) {
//					useOffset = true;
//					topValues = new TreeMap<byte[], byte[]>();
//				}
//			}
//		}
		this.isAscending = isAscending;

	}
}
