# <img src="https://uploads-ssl.webflow.com/5ea5d3315186cf5ec60c3ee4/5edf1c94ce4c859f2b188094_logo.svg" alt="Pip.Services Logo" width="200"> <br/> ElasticSearch components for Pip.Services in Java

This module is a part of the [Pip.Services](http://pipservices.org) polyglot microservices toolkit.

The Elasticsearch module contains logging components with data storage on the Elasticsearch server.

The module contains the following packages:
- **Build** - contains a factory for the construction of components
- **Log** - Logging components

<a name="links"></a> Quick links:

* [Configuration](http://docs.pipservices.org/toolkit/getting_started/configurations/)
* [Virtual memory configuration](https://www.elastic.co/guide/en/elasticsearch/reference/current/vm-max-map-count.html)
* [API Reference](https://pip-services3-java.github.io/pip-services3-elasticsearch-java/)
* [Change Log](CHANGELOG.md)
* [Get Help](http://docs.pipservices.org/get_help/)
* [Contribute](http://docs.pipservices.org/contribute/)

## Use

Go to the pom.xml file in Maven project and add dependencies::
```xml
<dependency>
  <groupId>org.pipservices3</groupId>
  <artifactId>pip-services3-elasticsearch</artifactId>
  <version>3.0.0</version>
</dependency>
```

Microservice components shall perform logging usual way using CompositeLogger component.
The CompositeLogger will get ElasticSearchLogger from references and will redirect log messages
there among other destinations.

```java
import org.pipservices3.commons.config.ConfigParams;
import org.pipservices3.commons.config.IConfigurable;
import org.pipservices3.commons.refer.IReferenceable;
import org.pipservices3.commons.refer.IReferences;
import org.pipservices3.components.log.CompositeLogger;

public class MyComponent implements IConfigurable, IReferenceable {
    private CompositeLogger _logger = new CompositeLogger();

    public void configure(ConfigParams config) {
        this._logger.configure(config);
    }

    public void  setReferences(IReferences refs) {
        this._logger.setReferences(refs);
    }

    public void myMethod(String correlationId, Object param1) {
        this._logger.trace(correlationId, "Executed method mycomponent.mymethod");
        // ....
    }
}
```

Configuration for your microservice that includes ElasticSearch logger may look the following way.

```yaml
...
{{#if ELASTICSEARCH_ENABLED}}
- descriptor: pip-services:logger:elasticsearch:default:1.0
  connection:
    uri: {{{ELASTICSEARCG_SERVICE_URI}}}
    host: {{{ELASTICSEARCH_SERVICE_HOST}}}{{#unless ELASTICSEARCH_SERVICE_HOST}}localhost{{/unless}}
    port: {{ELASTICSEARCG_SERVICE_PORT}}{{#unless ELASTICSEARCH_SERVICE_PORT}}9200{{/unless}}\ 
{{/if}}
...
```

## Develop

For development you shall install the following prerequisites:
* Java SE Development Kit 18+
* Eclipse Java Photon or another IDE of your choice
* Docker
* Apache Maven

Build the project:
```bash
mvn install
```

Run automated tests:
```bash
mvn test
```

Generate API documentation:
```bash
./docgen.ps1
```

Before committing changes run dockerized build and test as:
```bash
./build.ps1
./test.ps1
./clear.ps1
```

## Contacts

The initial implementation is done by
**Sergey Seroukhov**
**Danil Prisiazhnyi**
