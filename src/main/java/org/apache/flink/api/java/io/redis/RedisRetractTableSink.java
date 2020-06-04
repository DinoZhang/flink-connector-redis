
package org.apache.flink.api.java.io.redis;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.table.api.types.DataType;
import org.apache.flink.table.api.types.DataTypes;
import org.apache.flink.table.sinks.RetractStreamTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.util.TableConnectorUtil;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.flink.util.InstantiationUtil;


/**
 * @version $Id: RedisRetractTableSink.java, v 0.1 2019年12月04日 10:43 AM Exp $
 */
public class RedisRetractTableSink implements RetractStreamTableSink<Row> {
	private final RedisOutFormat outputFormat;

	private String[] fieldNames;
	private DataType[] fieldTypes;

	/**
	 * Setter method for property <tt>fieldNames</tt>.
	 *
	 * @param fieldNames value to be assigned to property fieldNames
	 */
	public void setFieldNames(final String[] fieldNames) {
		this.fieldNames = fieldNames;
	}

	/**
	 * Setter method for property <tt>fieldTypes</tt>.
	 *
	 * @param fieldTypes value to be assigned to property fieldTypes
	 */
	public void setFieldTypes(final DataType[] fieldTypes) {
		this.fieldTypes = fieldTypes;
	}

	public RedisRetractTableSink(final RedisOutFormat outputFormat) {
		this.outputFormat = outputFormat;
	}

	@Override
	public DataType getRecordType() {
		return null;
	}

	@Override
	public DataType getOutputType() {
		DataType outputType =  DataTypes.createRowType(fieldTypes, fieldNames);

		return DataTypes.createTupleType(DataTypes.BOOLEAN, outputType);
	}

	@Override
	public DataStreamSink<?> emitDataStream(final DataStream<Tuple2<Boolean, Row>> dataStream) {

		return dataStream.flatMap(new FlatMapFunction<Tuple2<Boolean, Row>, Row>() {
			@Override
			public void flatMap(final Tuple2<Boolean, Row> value, final Collector<Row> out) throws Exception {
				if (value.f0) {
					out.collect(value.f1);
				}

			}
		}).addSink(new RedisSinkFunction(outputFormat)).name(TableConnectorUtil.generateRuntimeName(this.getClass(), fieldNames));
	}

	@Override
	public String[] getFieldNames() {
		return fieldNames;
	}

	@Override
	public DataType[] getFieldTypes() {
		return fieldTypes;
	}

	@Override
	public TableSink<Tuple2<Boolean, Row>> configure(final String[] fieldNames, final DataType[] fieldTypes) {
		RedisRetractTableSink copy = null;
		try {
			copy = new RedisRetractTableSink(InstantiationUtil.clone(outputFormat));
			copy.setFieldTypes(fieldTypes);
			copy.setFieldNames(fieldNames);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		return copy;
	}
}
