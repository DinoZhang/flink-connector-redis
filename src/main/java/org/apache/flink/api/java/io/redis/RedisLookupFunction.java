package org.apache.flink.api.java.io.redis;

import org.apache.flink.table.api.functions.FunctionContext;
import org.apache.flink.table.api.functions.TableFunction;
import org.apache.flink.table.api.types.DataType;
import org.apache.flink.table.dataformat.BinaryString;
import org.apache.flink.types.Row;

/**
 * @version $Id: RedisLookupFunction.java, v 0.1 2019年11月24日 5:51 PM Exp $
 */
public class RedisLookupFunction extends TableFunction<Row> {

	private RedisInputFormat redisInputFormat;

	public RedisLookupFunction(final RedisInputFormat redisInputFormat) {
		this.redisInputFormat = redisInputFormat;
	}

	@Override
	public DataType getResultType(final Object[] arguments, final Class[] argTypes) {
		return super.getResultType(arguments, argTypes);
	}

	@Override
	public void open(final FunctionContext context) throws Exception {
		redisInputFormat.openInputFormat();
	}

	public void eval(Object... key) {
		//redisInputFormat.
		String result = redisInputFormat.getRedisConnection().get(BinaryString.fromString(key[0]).toString());
		if (result != null){
			Row row = new Row(2);
			row.setField(0, BinaryString.fromString(key[0]).toString());
			row.setField(1, result);
			collect(row);
		}
	}

	@Override
	public void close() throws Exception {
		super.close();
		redisInputFormat.close();
	}
}
