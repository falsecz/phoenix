package com.salesforce.phoenix.expression.aggregator;

import com.salesforce.phoenix.expression.Expression;
import com.salesforce.phoenix.expression.function.LengthFunction;
import com.salesforce.phoenix.parse.FunctionParseNode;
import com.salesforce.phoenix.schema.ColumnModifier;
import com.salesforce.phoenix.schema.PDataType;
import com.salesforce.phoenix.schema.tuple.Tuple;
import com.salesforce.phoenix.util.ByteUtil;
import com.salesforce.phoenix.util.FirstAggregatorDataTransferWrappper;
import java.util.List;
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
	protected int selectOffset = -1;
	protected WritableByteArrayComparable topOrder = new BinaryComparator(ByteUtil.EMPTY_BYTE_ARRAY);
	protected byte[] topValue = null;
	protected TreeMap<byte[], byte[]> topValues;
	protected boolean isAscending;
	private PDataType dataType = PDataType.VARBINARY;

	public FirstAndLastBaseClientAggregator(ColumnModifier columnModifier) {
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
	public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
		if (topValue == null) {
			return false;
		}

		if (useOffset) {
			//@TODO get offset from tree map
		} else {
			ptr.set(topValue);
		}
		return true;
	}

	@Override
	public void aggregate(Tuple tuple, ImmutableBytesWritable ptr) {
		if (useOffset) {
			//@TODO get top n values
		} else {

			byte[] messageFromRow = new byte[ptr.getSize()];
			System.arraycopy(ptr.get(), ptr.getOffset(), messageFromRow, 0, ptr.getLength());

			FirstAggregatorDataTransferWrappper payload = new FirstAggregatorDataTransferWrappper();
			payload.setBytesMessage(messageFromRow);

			byte[] currentValue = payload.getValue();
			byte[] currentOrder = payload.getOrderValue();
			isAscending = payload.getIsAscending();


			boolean isBetter;
			if (isAscending) {
				isBetter = topOrder.compareTo(currentOrder) > 0; //asc
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

	public void init(List<Expression> children) {
		this.children = children;
	}
}
