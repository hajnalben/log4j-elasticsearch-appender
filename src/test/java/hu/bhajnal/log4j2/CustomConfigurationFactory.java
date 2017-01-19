package hu.bhajnal.log4j2;

import java.net.URI;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.ConfigurationFactory;
import org.apache.logging.log4j.core.config.ConfigurationSource;
import org.apache.logging.log4j.core.config.Order;
import org.apache.logging.log4j.core.config.builder.api.AppenderComponentBuilder;
import org.apache.logging.log4j.core.config.builder.api.ConfigurationBuilder;
import org.apache.logging.log4j.core.config.builder.impl.BuiltConfiguration;
import org.apache.logging.log4j.core.config.plugins.Plugin;

@Plugin(name = "CustomConfigurationFactory", category = ConfigurationFactory.CATEGORY)
@Order(50)
public class CustomConfigurationFactory extends ConfigurationFactory {

	static Configuration createConfiguration(
		final String name, ConfigurationBuilder<BuiltConfiguration> builder) {

		builder.setConfigurationName(name);
		builder.setStatusLevel(Level.INFO);
		builder.setPackages("hu.bhajnal.log4j2");

		AppenderComponentBuilder appenderBuilder = builder.newAppender("elastic",
			"ElasticsearchAppender").addAttribute("host", "nurlog").addAttribute("cluster",
				"nurlog");

		builder.add(appenderBuilder);

		// IncludeLocation is required for getting class, method and line
		builder.add(builder.newRootLogger(Level.INFO).addAttribute("includeLocation", true).add(
			builder.newAppenderRef("elastic")));

		return builder.build();
	}

	@Override
	public Configuration getConfiguration(
		final LoggerContext loggerContext, final ConfigurationSource source) {

		return getConfiguration(loggerContext, source.toString(), null);
	}

	@Override
	public Configuration getConfiguration(
		final LoggerContext loggerContext, final String name, final URI configLocation) {

		ConfigurationBuilder<BuiltConfiguration> builder = newConfigurationBuilder();

		return createConfiguration(name, builder);
	}

	@Override
	protected String[] getSupportedTypes() {
		return new String[] { "*" };
	}
}