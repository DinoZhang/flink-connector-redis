package org.apache.flink.api.java.io.redis;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.types.Row;

/**
 * @version $Id: RedisSinkFunction.java, v 0.1 2019年12月04日 11:05 AM Exp $
 */
public class RedisSinkFunction extends RichSinkFunction<Row> {

	private RedisOutFormat redisOutFormat;
	private Counter counter;

	public RedisSinkFunction(final RedisOutFormat redisOutFormat) {
		this.redisOutFormat = redisOutFormat;
	}

	@Override
	public void invoke(final Row value, final Context context) throws Exception {
		redisOutFormat.writeRecord(value);
		counter.inc();
	}

	@Override
	public void open(final Configuration parameters) throws Exception {
		super.open(parameters);
		RuntimeContext ctx = getRuntimeContext();
		redisOutFormat.setRuntimeContext(ctx);
		this.counter = getRuntimeContext().getMetricGroup().counter("RedisSink");
	}

	@Override
	public void close() throws Exception {
		super.close();
	}
}
