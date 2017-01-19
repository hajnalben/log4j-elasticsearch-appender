package hu.bhajnal.log4j2;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.logging.log4j.core.Filter;
import org.apache.logging.log4j.core.Layout;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.config.plugins.PluginAttribute;
import org.apache.logging.log4j.core.config.plugins.PluginElement;
import org.apache.logging.log4j.core.config.plugins.PluginFactory;
import org.apache.logging.log4j.message.MapMessage;
import org.apache.logging.log4j.util.ReadOnlyStringMap;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

/**
 * Log4j2 appender class for Elasticsearch logging.
 *
 * @author bhajnal
 *
 */
@Plugin(name = "ElasticsearchAppender", category = "Core", elementType = "appender", printObject = true)
public class ElasticsearchAppender extends AbstractAppender {

	/**
	 * Creates a new ElasticsearchAppender which will be used by log4j2.
	 *
	 * @param name
	 *        the name of the appender
	 * @param filter
	 *        the Filter to associate with the Appender
	 * @param host
	 *        the host of the elasticsearch cluster
	 * @param port
	 *        the port of the elasticsearch cluster
	 * @param cluster
	 *        the elasticsearch cluster name
	 * @param index
	 *        the elastic index the logs will be sent to
	 * @param type
	 *        the elastic type the logs will be sent to
	 * @param buffer
	 *        the maximum unprocessed logs stored in memory
	 * @param bulkSize
	 *        the maximum number of logs sent to the elastic by a bulk request
	 * @return
	 */
	@PluginFactory
	public static ElasticsearchAppender createAppender(
		@PluginAttribute("name") String name, @PluginElement("Filter") final Filter filter,
		@PluginElement("Layout") Layout<?> layout,
		@PluginAttribute(value = "host", defaultString = "localhost") String host,
		@PluginAttribute(value = "port", defaultInt = 9300) int port,
		@PluginAttribute(value = "cluster", defaultString = "logstash") String cluster,
		@PluginAttribute(value = "index", defaultString = "logstash") String index,
		@PluginAttribute(value = "type", defaultString = "log") String type,
		@PluginAttribute(value = "buffer", defaultInt = 5000) int buffer,
		@PluginAttribute(value = "bulk-size", defaultInt = 500) int bulkSize) {

		if (name == null) {
			LOGGER.error("Please provide a name for ElasticsearchAppender!");

			return null;
		}

		Config config = new Config();

		config.name = name;
		config.host = host;
		config.port = port;
		config.cluster = cluster;
		config.index = index;
		config.type = type;
		config.buffer = buffer;
		config.bulkSize = bulkSize;

		return new ElasticsearchAppender(name, filter, layout, config);
	}

	/**
	 * Adds the logevent to the end of the event queue
	 */
	@Override
	public void append(LogEvent logEvent) {
		_eventQueue.offer(_prepareLogEvent(logEvent));
	}

	/**
	 * Starts a new workerthread to for the bulk processing
	 */
	@Override
	public void start() {
		_isRunning = true;

		Thread thr = new Thread() {

			@Override
			public void run() {
				_worker();
			}

		};

		thr.setDaemon(true);
		thr.setName("ElasticsearchAppender-" + _config.name);
		thr.start();

		setStarted();
	}

	@Override
	public void stop() {
		_isRunning = false;
		_client.close();
	}

	protected ElasticsearchAppender(String name, Filter filter, Layout<?> layout, Config config) {
		super(name, filter, layout);

		_config = config;

		_eventQueue = new ArrayBlockingQueue<>(config.buffer);

		try {
			_client = _buildClient();
		}
		catch (UnknownHostException e) {
			LOGGER.error("Failed to instantiate elastic client", e);

			throw new RuntimeException(e);
		}
	}

	private static Map<String, Object> _prepareLogEvent(LogEvent logEvent) {
		Map<String, Object> log = new HashMap<>();

		log.put("@timestamp", _ELASTIC_DATE_FORMAT.format(new Date(logEvent.getTimeMillis())));
		log.put("level", logEvent.getLevel().toString());

		if (logEvent.getMessage() instanceof MapMessage) {
			log.putAll(((MapMessage)logEvent.getMessage()).getData());
		}
		else {
			log.put("message", logEvent.getMessage().getFormattedMessage());
		}

		try {
			log.put("host", InetAddress.getLocalHost().getCanonicalHostName());
		}
		catch (UnknownHostException e) {
			LOGGER.warn("Unknown local hostname");
		}

		// Warning: includeLocation=true is required on the RootLogger
		StackTraceElement source = logEvent.getSource();

		if (source != null) {
			log.put("class", source.getClassName());
			log.put("method", source.getMethodName());
			log.put("line", source.getLineNumber());
		}

		if (logEvent.getThrown() != null) {
			log.put("exception", logEvent.getThrown().getClass().getName());
			log.put("stackTrace", ExceptionUtils.getStackTrace(logEvent.getThrown()));
		}

		ReadOnlyStringMap contextData = logEvent.getContextData();

		Set<Entry<String, String>> contextEntrySet = contextData.toMap().entrySet();

		for (Entry<String, String> entry : contextEntrySet) {
			log.put(entry.getKey(), entry.getValue());
		}

		return log;
	}

	private TransportClient _buildClient() throws UnknownHostException {
		InetAddress inetAddress = InetAddress.getByName(_config.host);
		InetSocketTransportAddress address = new InetSocketTransportAddress(inetAddress,
			_config.port);

		Settings settings = Settings.settingsBuilder().put("cluster.name", _config.cluster).build();

		return TransportClient.builder().settings(settings).build().addTransportAddress(address);
	}

	private void _processEvents(List<Map<String, Object>> events) {
		BulkRequestBuilder bulkRequest = _client.prepareBulk();

		for (Map<String, Object> logEvent : events) {
			IndexRequestBuilder indexRequest = _client.prepareIndex(_config.index, _config.type);
			bulkRequest.add(indexRequest.setSource(logEvent));
		}

		// Wait for the indexing to return
		BulkResponse bulkResponse = bulkRequest.get();

		if (bulkResponse.hasFailures()) {
			LOGGER.error(bulkResponse.buildFailureMessage());
		}
	}

	private void _worker() {
		while (_isRunning) {
			List<Map<String, Object>> events = new ArrayList<>();

			_eventQueue.drainTo(events, _config.bulkSize);

			if (!events.isEmpty()) {
				_processEvents(events);
			}

			try {
				Thread.sleep(500);
			}
			catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	private static final SimpleDateFormat _ELASTIC_DATE_FORMAT = new SimpleDateFormat(
		"yyyy-MM-dd'T'HH:mm:ss.SSSZ");

	private Client _client;

	private Config _config;

	private ArrayBlockingQueue<Map<String, Object>> _eventQueue;

	private boolean _isRunning;

	private static class Config {

		public String index;

		public String type;

		private int buffer;

		private int bulkSize;

		private String cluster;

		private String host;

		private String name;

		private int port;

	}

}
