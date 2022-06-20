package org.pipservices3.elasticsearch.log;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.http.HttpHost;

import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.*;
import org.elasticsearch.client.indices.CreateIndexRequest;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xcontent.XContentType;
import org.pipservices3.commons.config.ConfigParams;
import org.pipservices3.commons.convert.JsonConverter;
import org.pipservices3.commons.data.IdGenerator;
import org.pipservices3.commons.errors.ApplicationException;
import org.pipservices3.commons.errors.ConfigException;
import org.pipservices3.commons.errors.InvocationException;
import org.pipservices3.commons.refer.IReferenceable;
import org.pipservices3.commons.refer.IReferences;
import org.pipservices3.commons.run.IOpenable;
import org.pipservices3.components.log.CachedLogger;
import org.pipservices3.components.log.LogMessage;
import org.pipservices3.rpc.connect.HttpConnectionResolver;


import java.io.IOException;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

public class ElasticSearchLogger extends CachedLogger implements IReferenceable, IOpenable {

    private final HttpConnectionResolver _connectionResolver = new HttpConnectionResolver();

    private Timer _timer;
    private String _index = "log";
    private String _dateFormat = "YYYYMMDD";
    private boolean _dailyIndex = false;
    private String _currentIndex;
    private int _reconnect = 60000;
    private int _timeout = 30000;
    private int _maxRetries = 3;
    private boolean _indexMessage = false;
    private boolean _include_type_name = false;
    private RestHighLevelClient _client = null;

    /**
     * Configures component by passing configuration parameters.
     *
     * @param config configuration parameters to be set.
     */
    @Override
    public void configure(ConfigParams config) {
        super.configure(config);

        this._connectionResolver.configure(config);

        this._index = config.getAsStringWithDefault("index", this._index);
        this._dateFormat = config.getAsStringWithDefault("date_format", this._dateFormat);
        this._dailyIndex = config.getAsBooleanWithDefault("daily", this._dailyIndex);
        this._reconnect = config.getAsIntegerWithDefault("options.reconnect", this._reconnect);
        this._timeout = config.getAsIntegerWithDefault("options.timeout", this._timeout);
        this._maxRetries = config.getAsIntegerWithDefault("options.max_retries", this._maxRetries);
        this._indexMessage = config.getAsBooleanWithDefault("options.index_message", this._indexMessage);
        this._include_type_name = config.getAsBooleanWithDefault("options.include_type_name", this._include_type_name);

        if (_dateFormat.contains("DD"))
            _dateFormat = _dateFormat.replace("DD", "dd");
    }

    /**
     * Sets references to dependent components.
     *
     * @param references references to locate the component dependencies.
     */
    @Override
    public void setReferences(IReferences references) {
        super.setReferences(references);
        this._connectionResolver.setReferences(references);
    }

    /**
     * Checks if the component is opened.
     *
     * @return true if the component has been opened and false otherwise.
     */
    @Override
    public boolean isOpen() {
        return this._timer != null;
    }

    /**
     * Opens the component.
     *
     * @param correlationId (optional) transaction id to trace execution through call chain.
     */
    @Override
    public void open(String correlationId) throws ApplicationException {
        if (this.isOpen())
            return;


        var connection = this._connectionResolver.resolve(correlationId);
        if (connection == null) {
            throw new ConfigException(
                    correlationId,
                    "NO_CONNECTION",
                    "Connection is not configured"
            );
        }

//        RestClientBuilder builder = RestClient.builder(
//                new HttpHost(connection.getHost(), connection.getPort(), "http"));

        RestClientBuilder builder = RestClient.builder(new HttpHost(connection.getHost(), connection.getPort(), "http"));

        builder.setRequestConfigCallback(
                builder1 -> builder1
                        .setConnectTimeout(_timeout)
                        .setSocketTimeout(_reconnect)
                        .setMaxRedirects(_maxRetries));

        _client = new RestHighLevelClient(builder);

        this.createIndexIfNeeded(correlationId, true);

        _timer = new Timer();

        _timer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                dump();
            }
        }, 0, _interval);
    }

    @Override
    public void close(String correlationId) throws InvocationException {
        this.save(this._cache);

        if (this._timer != null)
            this._timer.cancel();

        this._cache = new ArrayList<>();
        this._timer = null;
        this._client = null;
    }

    private String getCurrentIndex() {
        if (!this._dailyIndex) return this._index;

        var datePattern = ZonedDateTime.now().format(DateTimeFormatter.ofPattern(_dateFormat));

        return this._index + "-" + datePattern;
    }

    private void createIndexIfNeeded(String correlationId, boolean force) {
        var newIndex = this.getCurrentIndex();
        if (!force && Objects.equals(this._currentIndex, newIndex)) {
            return;
        }

        this._currentIndex = newIndex;

        GetIndexRequest request = new GetIndexRequest();
        request.indices(_currentIndex);

        boolean exists;

        try {
            exists = _client.indices().exists(request, RequestOptions.DEFAULT);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        try {
            if (exists)
                return;

            var createRequest = new CreateIndexRequest(_currentIndex);

            createRequest.settings(Settings.builder()
                    .put("index.number_of_shards", 1)
            );

            createRequest.mapping(this.getIndexSchema());

            _client.indices().create(
                    createRequest, RequestOptions.DEFAULT
            );
        } catch (IOException err) {
            // Skip already exist errors
            if (!err.getMessage().contains("resource_already_exists"))
                throw new RuntimeException(err);
        }
    }

    private Map<String, ?> getIndexSchema() {
        final Map<String, ?> schema = Map.of(
                "properties", Map.of(
                        "time", Map.of("type", "date", "index", true),
                        "source", Map.of("type", "keyword", "index", true),
                        "level", Map.of("type", "keyword", "index", true),
                        "correlation_id", Map.of("type", "text", "index", true),
                        "error", Map.of(
                                "type", "object",
                                "properties", Map.of(
                                        "type", Map.of("type", "keyword", "index", true),
                                        "category", Map.of("type", "keyword", "index", true),
                                        "status", Map.of("type", "integer", "index", false),
                                        "code", Map.of("type", "keyword", "index", true),
                                        "message", Map.of("type", "text", "index", false),
                                        "details", Map.of("type", "object"),
                                        "correlation_id", Map.of("type", "text", "index", false),
                                        "cause", Map.of("type", "text", "index", false),
                                        "stack_trace", Map.of("type", "text", "index", false)
                                )
                        ),
                        "message", Map.of("type", "text", "index", this._indexMessage)
                )
        );

        if (this._include_type_name) {
            return Map.of("log_message", schema);
        } else return schema;
    }

    /**
     * Saves log messages from the cache.
     *
     * @param messages a list with log messages
     */
    @Override
    protected void save(List<LogMessage> messages) throws InvocationException {
        if (!this.isOpen() || messages.size() == 0)
            return;

        this.createIndexIfNeeded("elasticsearch_logger", false);

        var bulkRequest = new BulkRequest();
        for (var message : messages) {
            var req = getLogItem();

            try {
                req.source(JsonConverter.toMap(JsonConverter.toJson(message)));
                bulkRequest.add(req);
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        }

        try {
            _client.bulk(bulkRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    protected IndexRequest getLogItem() {
        if (this._include_type_name)
            return new IndexRequest(_currentIndex, "log_message", IdGenerator.nextLong()); // ElasticSearch 6.x
        else
            return new IndexRequest(_currentIndex).id(IdGenerator.nextLong()); // ElasticSearch 7.x
    }
}
