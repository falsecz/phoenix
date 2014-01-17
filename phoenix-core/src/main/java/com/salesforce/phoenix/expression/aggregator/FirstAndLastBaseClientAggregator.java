package com.salesforce.phoenix.expression.aggregator;

import com.salesforce.phoenix.expression.Expression;
import com.salesforce.phoenix.expression.function.LengthFunction;
import com.salesforce.phoenix.parse.FunctionParseNode;
import com.salesforce.phoenix.schema.ColumnModifier;
import com.salesforce.phoenix.schema.PDataType;
import com.salesforce.phoenix.schema.tuple.SingleKeyValueTuple;
import com.salesforce.phoenix.schema.tuple.Tuple;
import com.salesforce.phoenix.util.BinarySerializableComparator;
import com.salesforce.phoenix.util.ByteUtil;
import com.salesforce.phoenix.util.FirstAggregatorDataTransferWrappper;
import com.salesforce.phoenix.util.FirstByLastByWithOffsetWrapper;
import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.TreeMap;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.WritableByteArrayComparable;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

/**
 *
 * @author tzolkincz
 */
@FunctionParseNode.BuiltInFunction(name = LengthFunction.NAME, args = {
	@FunctionParseNode.Argument(allowedTypes = {PDataType.VARBINARY})})
public class FirstAndLastBaseClientAggregator extends BaseAggregator {

	protected final ImmutableBytesWritable value = new ImmutableBytesWritable(ByteUtil.EMPTY_BYTE_ARRAY);
	protected List<Expression> children;
	protected boolean useOffset = false;
	protected int offset = -1;
	protected int selectOffset = -1;
	protected WritableByteArrayComparable topOrder = new BinaryComparator(ByteUtil.EMPTY_BYTE_ARRAY);
	protected byte[] topValue = null;
	protected TreeMap<byte[], byte[]> topValues = new TreeMap<byte[], byte[]>(new BinarySerializableComparator());
	protected boolean isAscending;
	private final PDataType dataType = PDataType.VARBINARY;

	public FirstAndLastBaseClientAggregator(ColumnModifier columnModifier) {
		super(columnModifier);
	}

	@Override
	public void reset() {
		value.set(ByteUtil.EMPTY_BYTE_ARRAY);
		topOrder = new BinaryComparator(ByteUtil.EMPTY_BYTE_ARRAY);
		topValue = null;
//		offset = -1;
//		useOffset = false;
		topValues.clear();

		super.reset();
	}

	@Override
	public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
		if (useOffset) {
			if (topValues.size() == 0) {
				return false;
			}

			Set<byte[]> keySet;
			if (isAscending) {
				keySet = topValues.keySet();
			} else {
				keySet = topValues.descendingKeySet();
			}

			int counter = offset;
			for (byte[] currentKey : keySet) {
				if (counter-- == 1) {
					ptr.set(topValues.get(currentKey));
					return true;
				}
			}
			return false;
		}

		if (topValue == null) {
			return false;
		}

		ptr.set(topValue);
		return true;
	}

	@Override
	public void aggregate(Tuple tuple, ImmutableBytesWritable ptr) {
		if (useOffset) {
			FirstByLastByWithOffsetWrapper payload = new FirstByLastByWithOffsetWrapper();
			try {
				payload.setPayload(ptr.copyBytes());

				topValues.putAll(payload.getData());
				isAscending = payload.getIsAscending();
			} catch (Exception ex) {
				//@TODO do some thing here
				System.out.println("jebka");
			}
		} else {
			//if is called cause aggregation in ORDER BY clausule
			if (tuple instanceof SingleKeyValueTuple) {
				topValue = ptr.copyBytes();
				return;
			}

			byte[] messageFromRow = new byte[ptr.getSize()];
			System.arraycopy(ptr.get(), ptr.getOffset(), messageFromRow, 0, ptr.getLength());

			FirstAggregatorDataTransferWrappper payload = new FirstAggregatorDataTransferWrappper();
			payload.setBytesMessage(messageFromRow);

			byte[] currentValue = payload.getValue();
			byte[] currentOrder = payload.getOrderValue();
			isAscending = payload.getIsAscending();

			boolean isBetter;
			if (isAscending) {
				isBetter = topOrder.compareTo(currentOrder) > 0;
			} else {
				isBetter = topOrder.compareTo(currentOrder) < 0; //desc
			}
			if (topOrder.getValue().length < 1 || isBetter) {
				topOrder = new BinaryComparator(currentOrder);
				topValue = currentValue;
			}
		}
	}

	@Override
	public PDataType getDataType() {
		return dataType;
	}

	public void init(List<Expression> children, int offset) {
		if (offset != 0) {
			useOffset = true;
			this.offset = offset;
		}
		this.children = children;
	}
}
