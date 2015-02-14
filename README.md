## RxMule - Reactive Extensions for Mule ESB

[![Build Status](https://travis-ci.org/ddossot/RxMule.svg)](https://travis-ci.org/ddossot/RxMule)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/net.dossot/rx-mule/badge.svg)](https://maven-badges.herokuapp.com/maven-central/net.dossot/rx-mule)

[Mule ESB](http://www.mulesoft.com/platform/soa/mule-esb-open-source-esb) specific bindings for [RxJava](http://github.com/ReactiveX/RxJava).

The following demonstrates an asynchronous HTTP to Redis bridge, that only accepts one request per remote IP:

```java
rxMule
    .observeEndpointAsync(new URI("http://localhost:8080/publish"))
    .distinct(
        muleEvent -> {

            final String remoteAddressAndPort =
                muleEvent
                    .getMessage()
                    .getInboundProperty(
                        "MULE_REMOTE_CLIENT_ADDRESS");

            return substringBefore(remoteAddressAndPort, ":");
        })
    .subscribe(
        asAction((MessageConsumer)
            muleEvent -> {

                redisModule.publish(
                    "http-requests",
                    false,
                    muleEvent.getMessageAsString(),
                    muleEvent);

                LOGGER.info("Published: {}", muleEvent);
    }));
```

## Overview

This module adds a number of classes to RxJava that make it possible to observe:

- Mule inbound endpoints from traditional [transports](http://www.mulesoft.org/documentation/display/current/Transports+Reference),
  including global endpoints and endpoints defined by URIs,
- raw message sources, like the new [HTTP Listener Connector](http://www.mulesoft.org/documentation/display/current/HTTP+Listener+Connector),
- [Anypoint Connectors](http://www.mulesoft.com/platform/cloud-connectors) message sources.

RxMule also provides `Func1` and `Action1` wrappers to help processing Mule events with outbound Mule endpoints or Anypoint Connectors methods.
On top of that RxMule, provides helpers for using message transformers and creating new messages from scratch.

In short, RxMule allows creating `Observable<MuleEvent>` instances from different sources.
A [MuleEvent](https://www.mulesoft.org/docs/site/current3/apidocs/index.html?org/mule/api/MuleEvent.html) is what Mule creates and processes.
It wraps a [MuleMessage](https://www.mulesoft.org/docs/site/current3/apidocs/index.html?org/mule/api/MuleMessage.html) which contains the actual
data and meta-data that's being processed. Keep in mind that both objects are mutable, though only by the thread that is _owning_ the event.

> You can read more about the structure of a `MuleMessage` [here](http://www.mulesoft.org/documentation/display/current/Mule+Message+Structure).

Take a look at the [integration tests](https://github.com/ddossot/RxMule/blob/master/src/test/java/org/mule/rx/RxMuleITCase.java)
to have a better idea of all you can do with RxMule.


## Usage

Releases are available on Central.
Snapshot builds are available in the Sonatype OSS Snapshots repository:

```xml
<repository>
    <id>ossrh</id>
    <url>https://oss.sonatype.org/content/repositories/snapshots</url>
    <snapshots>
        <enabled>true</enabled>
    </snapshots>
</repository>
```

If you need to build the latest snapshot yourself, run:

    mvn clean install

Note that some integration tests require a local instance of Redis running on port `6379`.
If Redis is not available there, these tests will self-disable.

> You can quickly start a local Redis for tests with: `docker run -d -p 6379:6379 --name redis redis`
  and connect to it with: `docker run -it --link redis:redis --rm redis sh -c 'exec redis-cli -h "$REDIS_PORT_6379_TCP_ADDR" -p "$REDIS_PORT_6379_TCP_PORT"'`


**Copyright © 2015 David Dossot - MIT License**
