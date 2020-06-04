package org.apache.flink.api.java.io.redis;

import org.apache.flink.api.common.io.RichInputFormat;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.core.io.InputSplitAssigner;
import org.apache.flink.hive.shaded.org.apache.commons.lang3.StringUtils;
import org.apache.flink.types.Row;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

import java.io.IOException;

/**
 * @version $Id: RedisInputFormat.java, v 0.1 2019年11月29日 11:05 AM Exp $
 */
public class RedisInputFormat extends RichInputFormat<Row, InputSplit > implements ResultTypeQueryable<Row> {

	private static final long serialVersionUID = -5714715362124636647L;
	private RowTypeInfo rowTypeInfo;
	private static Logger log = LoggerFactory.getLogger(RedisOutFormat.class);
	private Jedis jedis;
	private String host;
	private int port;
	private String password;

	public RedisInputFormat(final String host, final int port, final String password) {
		this.host = host;
		this.port = port;
		this.password = password;
	}

	@Override
	public void configure(final Configuration parameters) {

	}

	@Override
	public BaseStatistics getStatistics(final BaseStatistics cachedStatistics) throws IOException {
		return null;
	}

	@Override
	public InputSplit[] createInputSplits(final int minNumSplits) throws IOException {
		return new InputSplit[0];
	}

	@Override
	public InputSplitAssigner getInputSplitAssigner(final InputSplit[] inputSplits) {
		return null;
	}

	@Override
	public void open(final InputSplit split) throws IOException {
	}

	@Override
	public boolean reachedEnd() throws IOException {
		return false;
	}

	@Override
	public Row nextRecord(final Row reuse) throws IOException {
		return null;
	}

	@Override
	public void close() throws IOException {
		jedis.close();
	}

	@Override
	public TypeInformation<Row> getProducedType() {
		return rowTypeInfo;
	}

	public Jedis getRedisConnection(){
		return jedis;
	}

	@Override
	public void openInputFormat() throws IOException {
		try {
			jedis = new Jedis(host, port);
			if (StringUtils.isNotBlank(password)) {
				jedis.auth(password);
			}

		} catch (Exception se) {
			throw new IllegalArgumentException("redis input open() failed." + se.getMessage(), se);
		}
	}

	@Override
	public void closeInputFormat() throws IOException {
		if (jedis != null){
			jedis.close();
		}
	}
}
