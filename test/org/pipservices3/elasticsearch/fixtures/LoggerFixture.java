package org.pipservices3.elasticsearch.fixtures;

import org.junit.Test;
import org.pipservices3.components.log.CachedLogger;
import org.pipservices3.components.log.LogLevel;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class LoggerFixture {
    private final CachedLogger _logger;

    public LoggerFixture(CachedLogger logger) {
        this._logger = logger;
    }

    public void testLogLevel() {
        assertTrue(this._logger.getLevel().getValue() >= LogLevel.None.getValue());
        assertTrue(this._logger.getLevel().getValue() <= LogLevel.Trace.getValue());
    }

    public void testSimpleLogging() throws InterruptedException {
        this._logger.setLevel(LogLevel.Trace);

        this._logger.fatal("", "", "Fatal error message");
        this._logger.error("", "", "Error message");
        this._logger.warn(null, "Warning message");
        this._logger.info(null, "Information message");
        this._logger.debug(null, "Debug message");
        this._logger.trace(null, "Trace message");

        this._logger.dump();

        Thread.sleep(1000);
    }

    public void testErrorLogging() throws InterruptedException {
        Exception err;
        try {
            // Raise an exception
            throw new Exception();
        } catch (Exception ex) {
            err = ex;
            this._logger.fatal("123", ex, "Fatal error");
            this._logger.error("123", ex, "Recoverable error");
        }

        assertNotNull(err);

        this._logger.dump();

        Thread.sleep(1000);
    }
}
