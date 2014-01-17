package com.salesforce.phoenix.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.util.TreeMap;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author tzolkincz
 */
public class FirstByLastByWithOffsetWrapper extends FirstAggregatorDataTransferWrappper {

	protected int offset;
	private TreeMap<byte[], byte[]> data;

	public void setOffset(int offset) {
		this.offset = offset;
	}

	public int getOffset() {
		return offset;
	}

	public void setData(TreeMap<byte[], byte[]> data) {
		this.data = data;
	}

	public byte[] getPayload() throws IOException {

		ByteArrayOutputStream bos = new ByteArrayOutputStream();

		bos.write(useCompression ? (byte) 1 : (byte) 0);
		bos.write(isAscending ? (byte) 1 : (byte) 0);
		bos.write(toByteArray(offset));

		ObjectOutput out = null;
		try {
			out = new ObjectOutputStream(bos);
			out.writeObject(data);

			byte[] payload = bos.toByteArray();
			return payload;

		} finally {
			out.close();
			bos.close();
		}
	}

	public void setPayload(byte[] payload) throws IOException {

		ByteArrayInputStream bis = new ByteArrayInputStream(payload);

		useCompression = bis.read() != 0;
		isAscending = bis.read() != 0;
		//byte[] offsetAsByte = null;

		byte[] foo = new byte [4];
		for (int i = 0; i < foo.length; i++) {
			byte[] a = new byte[1];
			bis.read(a);
			foo[i] = a[0];
		}

		//bis.read(offsetAsByte, 0, 4);

		offset = fromByteArray(foo);

		ObjectInput in = null;
		try {
			in = new ObjectInputStream(bis);
			data = (TreeMap)in.readObject();
		} catch (ClassNotFoundException ex) {
			Logger.getLogger(FirstByLastByWithOffsetWrapper.class.getName()).log(Level.SEVERE, null, ex);
		} finally {
			bis.close();
			in.close();
		}

	}

	public TreeMap<byte[], byte[]> getData() {
		return data;
	}

	private byte[] toByteArray(int value) {
		return new byte[]{
			(byte) (value >> 24),
			(byte) (value >> 16),
			(byte) (value >> 8),
			(byte) value};
	}

	private int fromByteArray(byte[] bytes) {
		return ByteBuffer.wrap(bytes).getInt();
	}
}
