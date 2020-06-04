package org.apache.flink.api.java.io.redis;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.RichTableSchema;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.functions.AsyncTableFunction;
import org.apache.flink.table.api.functions.TableFunction;
import org.apache.flink.table.api.types.DataType;
import org.apache.flink.table.plan.stats.TableStats;
import org.apache.flink.table.sources.LookupConfig;
import org.apache.flink.table.sources.LookupableTableSource;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.types.Row;

import java.util.Set;

/**
 * @version $Id: RedisTableSource.java, v 0.1 2019年12月04日 6:14 PM Exp $
 */
public class RedisTableSource implements LookupableTableSource<Row>, StreamTableSource<Row> {

	private RichTableSchema tableSchema; // for table type info


	private RedisInputFormat redisInputFormat;
	private Set<Set<String>> uniqueKeySet;
	private Set<Set<String>> indexKeySet;

	public RedisTableSource(final RichTableSchema tableSchema, final RedisInputFormat redisInputFormat, final Set<Set<String>> uniqueKeySet, final Set<Set<String>> indexKeySet) {
		this.tableSchema = tableSchema;
		this.redisInputFormat = redisInputFormat;
		this.uniqueKeySet = uniqueKeySet;
		this.indexKeySet = indexKeySet;
	}

	@Override
	public TableFunction<Row> getLookupFunction(final int[] lookupKeys) {
		return new RedisLookupFunction(redisInputFormat);
	}

	@Override
	public AsyncTableFunction<Row> getAsyncLookupFunction(final int[] lookupKeys) {
		return null;
	}

	@Override
	public LookupConfig getLookupConfig() {
		return null;
	}

	@Override
	public DataStream<Row> getDataStream(final StreamExecutionEnvironment execEnv) {
		return null;
	}

	@Override
	public DataType getReturnType() {
		return tableSchema.getResultRowType();
	}

	@Override
	public TableSchema getTableSchema() {
		TableSchema.Builder builder = TableSchema.builder();

		// uniqueKeySet.toArray(new String[uniqueKeySet.size()]);

		// HashSet[] uniqueKeySettemp = uniqueKeySet.toArray(new HashSet[uniqueKeySet.size()]);

		// indexKeySet.forEach(indexKey ->builder.uniqueIndex(indexKeySet.toArray(new String[indexKeySet.size()])));
		// builder.uniqueKey("pv");
		for (int i = 0; i < tableSchema.getColumnNames().length; i++) {

			builder.column(tableSchema.getColumnNames()[i], tableSchema.getColumnTypes()[i], tableSchema.getNullables()[i]);

		}

		/// ..foreach(c => builder.field(c.name, c.internalType, c.isNullable))

		if (uniqueKeySet != null) {
			uniqueKeySet.forEach(uniqueKey -> builder.uniqueIndex(uniqueKey.toArray((new String[0]))));
			indexKeySet.forEach(indexKey -> builder.normalIndex(indexKey.toArray((new String[0]))));
		}
		return new TableSchema(builder.build().getColumns(), builder.build().getPrimaryKeys(), builder.build().getUniqueKeys(), builder.build().getUniqueIndexes(), null, null);
	}

	@Override
	public String explainSource() {
		return "Redis";
	}

	@Override
	public TableStats getTableStats() {
		return null;
	}
}
