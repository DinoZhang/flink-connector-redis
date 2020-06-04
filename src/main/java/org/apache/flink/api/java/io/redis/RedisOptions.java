package org.apache.flink.api.java.io.redis;

import org.apache.flink.configuration.ConfigOption;

import java.util.Arrays;
import java.util.List;

import static org.apache.flink.configuration.ConfigOptions.key;



/**
 * Options for redis.
 */
public class RedisOptions {




	public static final ConfigOption<String> HOST = key("host".toLowerCase()).noDefaultValue();
	public static final ConfigOption<String> PORT = key("port".toLowerCase()).noDefaultValue();
	public static final ConfigOption<String> PASSWORD = key("password".toLowerCase()).noDefaultValue();
	public static final ConfigOption<String> BATH_SIZE = key("batchsize".toLowerCase()).noDefaultValue();
	public static final ConfigOption<String> DB = key("db".toLowerCase()).noDefaultValue();


	public static final List<String> SUPPORTED_KEYS = Arrays.asList(PASSWORD.key(), HOST.key(), PORT.key(), BATH_SIZE.key(), DB.key());
}
