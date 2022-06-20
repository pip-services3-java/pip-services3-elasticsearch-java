package org.pipservices3.elasticsearch.log;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.pipservices3.commons.config.ConfigParams;
import org.pipservices3.commons.errors.ApplicationException;
import org.pipservices3.commons.errors.InvocationException;
import org.pipservices3.elasticsearch.fixtures.LoggerFixture;

import static org.junit.Assert.assertTrue;

public class ElasticSearchLoggerTest {
    ElasticSearchLogger _logger;
    LoggerFixture _fixture;

    @Before
    public void setupClass() throws ApplicationException {
        var host = System.getenv("ELASTICSEARCH_SERVICE_HOST") != null ? System.getenv("ELASTICSEARCH_SERVICE_HOST") : "localhost";
        var port = System.getenv("ELASTICSEARCH_SERVICE_PORT") != null ? System.getenv("ELASTICSEARCH_SERVICE_PORT") : 9200;
        String dateFormat = "YYYYMMDD";

        _logger = new ElasticSearchLogger();
        _fixture = new LoggerFixture(_logger);

        var config = ConfigParams.fromTuples(
                "source", "test",
                "index", "log",
                "daily", true,
                "date_format", dateFormat,
                "connection.host", host,
                "connection.port", port
        );
        _logger.configure(config);

        _logger.open(null);
    }

    @After
    public void teardown() throws InvocationException {
        _logger.close(null);
    }

    @Test
    public void testLogLevel() {
        _fixture.testLogLevel();
    }

    @Test
    public void testSimpleLogging() throws InterruptedException {
        _fixture.testSimpleLogging();
    }

    @Test
    public void testErrorLogging() throws InterruptedException {
        _fixture.testErrorLogging();
    }

    /**
     * We test to ensure that the date pattern does not effect the opening of the elasticsearch component
     */
    @Test
    public void testDatePatternTestingYYYYMMDD() throws ApplicationException {
        var host = System.getenv("ELASTICSEARCH_SERVICE_HOST") != null ? System.getenv("ELASTICSEARCH_SERVICE_HOST") : "localhost";
        var port = System.getenv("ELASTICSEARCH_SERVICE_PORT") != null ? System.getenv("ELASTICSEARCH_SERVICE_PORT") : 9200;

        var logger = new ElasticSearchLogger();
        var dateFormat = "YYYY.MM.DD";

        var config = ConfigParams.fromTuples(
                "source", "test",
                "index", "log",
                "daily", true,
                "date_format", dateFormat,
                "connection.host", host,
                "connection.port", port
        );
        logger.configure(config);

        logger.open(null);

        // Since the currentIndex property is private, we will just check for an open connection
        assertTrue(logger.isOpen());
        logger.close(null);
    }

    /**
     * We test to ensure that the date pattern does not effect the opening of the elasticsearch component
     */
    @Test
    public void testDatePatternTestingYYYYMDD() throws ApplicationException {
        var host = System.getenv("ELASTICSEARCH_SERVICE_HOST") != null ? System.getenv("ELASTICSEARCH_SERVICE_HOST") : "localhost";
        var port = System.getenv("ELASTICSEARCH_SERVICE_PORT") != null ? System.getenv("ELASTICSEARCH_SERVICE_PORT") : 9200;
        String dateFormat = "YYYY.M.DD";

        var logger = new ElasticSearchLogger();

        var config = ConfigParams.fromTuples(
                "source", "test",
                "index", "log",
                "daily", true,
                "date_format", dateFormat,
                "connection.host", host,
                "connection.port", port
        );

        logger.configure(config);

        logger.open(null);

        // Since the currentIndex property is private, we will just check for an open connection
        assertTrue(logger.isOpen());
        logger.close(null);
    }
}
