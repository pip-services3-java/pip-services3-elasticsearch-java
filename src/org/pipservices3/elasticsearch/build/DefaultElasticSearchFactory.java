package org.pipservices3.elasticsearch.build;

import org.pipservices3.commons.refer.Descriptor;
import org.pipservices3.components.build.Factory;
import org.pipservices3.elasticsearch.log.ElasticSearchLogger;

/**
 * Creates ElasticSearch components by their descriptors.
 *
 * @see org.pipservices3.elasticsearch.log.ElasticSearchLogger
 */
public class DefaultElasticSearchFactory extends Factory {
    private static final Descriptor ElasticSearchLoggerDescriptor = new Descriptor("pip-services", "logger", "elasticsearch", "*", "1.0");

    /**
     * Create a new instance of the factory.
     */
    public DefaultElasticSearchFactory() {
        super();
        this.registerAsType(DefaultElasticSearchFactory.ElasticSearchLoggerDescriptor, ElasticSearchLogger.class);
    }
}
