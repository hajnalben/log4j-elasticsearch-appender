package hu.bhajnal.log4j2;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.ThreadContext;
import org.apache.logging.log4j.core.config.ConfigurationFactory;
import org.junit.BeforeClass;
import org.junit.Test;

public class ElasticsearchAppenderTestIT {

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		ConfigurationFactory.setConfigurationFactory(new CustomConfigurationFactory());

		_log = LogManager.getLogger(ElasticsearchAppenderTestIT.class);
	}

	@Test
	public void testLogging() throws InterruptedException {
		ThreadContext.put("ctx-var", "dummy value");

		_log.info("This is an info log.");

		try {
			throw new Exception("This is an exception!");
		}
		catch (Exception e) {
			_log.error("Exception catched", e);
		}

		_log.warn("A warning is here.");

		for (int i = 0; i < 400; i++) {
			_log.info("Log message # " + i);
		}

		Thread.sleep(500);

		for (int i = 400; i < 800; i++) {
			_log.warn("Log message # " + i);
		}

		// Wait for the bulk processor to finish
		Thread.sleep(5000);
	}

	private static Logger _log;

}
