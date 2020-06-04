package org.apache.flink.api.java.io.redis;

import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.hive.shaded.org.apache.commons.lang3.StringUtils;
import org.apache.flink.types.Row;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @version $Id: RedisOutFormat.java, v 0.1 2019年11月29日 11:05 AM Exp $
 */
public class RedisOutFormat extends RichOutputFormat<Row> {

	private static final long serialVersionUID = -3819462971300769382L;

	private static Logger log = LoggerFactory.getLogger(RedisOutFormat.class);
	private Jedis jedis;
	private List<Row> cache = new ArrayList<Row>();
	private int batchsize;
	private String host;
	private int port;
	private String password;
	private int db;

	public RedisOutFormat(final int batchsize, final String host, final int port, final String password, final int db) {
		this.batchsize = batchsize;
		this.host = host;
		this.port = port;
		this.password = password;
		this.db = db;
	}

	@Override
	public void configure(final Configuration parameters) {
	}

	@Override
	public void open(final int taskNumber, final int numTasks) throws IOException {
		log.info(String.format("Open Method Called: taskNumber %d numTasks %d", taskNumber, numTasks));
		batchsize = batchsize > 0 ? batchsize : 1;
		try {
			jedis = new Jedis(host, port);
			if (StringUtils.isNotBlank(password)) {
				jedis.auth(password);
			}
			jedis.select(db);
		} catch (Exception e) {
			throw new IllegalArgumentException("redis open() failed.", e);
		}

	}

	@Override
	public void writeRecord(final Row record) throws IOException {
		String key = record.getField(0).toString();
		String value = record.getField(1).toString();
		//log.info("key:" + key + ",value:" + value);
		try {
			cache.add(record);
			if (cache.size() >= batchsize) {
				for (int i = 0; i < cache.size(); i++) {
					jedis.set(key, value);
				}
				cache.clear();
			}
		} catch (Exception e) {
			throw new IllegalArgumentException("redis write() failed.", e);
		}

	}

	@Override
	public void close() throws IOException {
		try {
			jedis.quit();
			jedis.close();
		} catch (Exception e) {
			throw new IllegalArgumentException("redis close() failed.", e);
		}

	}
}
