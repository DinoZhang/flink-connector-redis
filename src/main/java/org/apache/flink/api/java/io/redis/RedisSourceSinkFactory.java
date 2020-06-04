package org.apache.flink.api.java.io.redis;

import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.table.api.RichTableSchema;
import org.apache.flink.table.factories.StreamTableSinkFactory;
import org.apache.flink.table.factories.StreamTableSourceFactory;
import org.apache.flink.table.sinks.StreamTableSink;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.table.util.TableProperties;
import org.apache.flink.types.Row;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.flink.api.java.io.redis.RedisOptions.SUPPORTED_KEYS;
import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_PROPERTY_VERSION;
import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_TYPE;

/**
 * @version $Id: RedisSourceSinkFactory.java, v 0.1 2019年11月29日 10:29 AM Exp $
 */
public class RedisSourceSinkFactory implements StreamTableSourceFactory<Row>, StreamTableSinkFactory<TupleTypeInfo> {
	@Override
	public StreamTableSink<TupleTypeInfo> createStreamTableSink(final Map<String, String> props) {
		TableProperties properties = new TableProperties();
		properties.putProperties(props);
		RichTableSchema schema = properties.readSchemaFromProperties(null);

		String host = properties.getString(RedisOptions.HOST);
		int port = properties.getInteger(RedisOptions.PORT.key(), 6349);
		String password = properties.getString(RedisOptions.PASSWORD);
		int db = properties.getInteger(RedisOptions.DB.key(), 0);
		int batchSize = properties.getInteger(RedisOptions.BATH_SIZE.key(), 1);

		RedisOutFormat redisOutFormat = new RedisOutFormat(batchSize, host, port, password, db);

		RedisRetractTableSink redisRetractTableSink = new RedisRetractTableSink(redisOutFormat);
		redisRetractTableSink.setFieldNames(schema.getColumnNames());
		redisRetractTableSink.setFieldTypes(schema.getColumnTypes());

		return (StreamTableSink) redisRetractTableSink.configure(schema.getColumnNames(), schema.getColumnTypes());
	}

	@Override
	public StreamTableSource<Row> createStreamTableSource(final Map<String, String> props) {
		TableProperties properties = new TableProperties();
		properties.putProperties(props);
		RichTableSchema schema = properties.readSchemaFromProperties(null);

		String host = properties.getString(RedisOptions.HOST);
		int port = properties.getInteger(RedisOptions.PORT.key(), 6349);
		String password = properties.getString(RedisOptions.PASSWORD);

		Set<Set<String>> uniqueKeys = new HashSet<>();
		Set<Set<String>> normalIndexes = new HashSet<>();
		if (!schema.getPrimaryKeys().isEmpty()) {
			uniqueKeys.add(new HashSet<>(schema.getPrimaryKeys()));
		}
		for (List<String> uniqueKey : schema.getUniqueKeys()) {
			uniqueKeys.add(new HashSet<>(uniqueKey));
		}
		for (RichTableSchema.Index index : schema.getIndexes()) {
			if (index.unique) {
				uniqueKeys.add(new HashSet<>(index.keyList));
			} else {
				normalIndexes.add(new HashSet<>(index.keyList));
			}
		}

		RedisInputFormat redisInputFormat = new RedisInputFormat(host, port, password);

		RedisTableSource redisTableSource = new RedisTableSource(schema, redisInputFormat, uniqueKeys, normalIndexes);

		return redisTableSource;
	}

	@Override
	public Map<String, String> requiredContext() {
		Map<String, String> context = new HashMap<>();
		context.put(CONNECTOR_TYPE, "REDIS");
		context.put(CONNECTOR_PROPERTY_VERSION, "1");
		return context;
	}

	@Override
	public List<String> supportedProperties() {
		return SUPPORTED_KEYS;
	}
}
