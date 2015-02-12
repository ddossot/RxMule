
package org.mule.rx;

import static com.jayway.restassured.RestAssured.given;
import static java.nio.file.StandardWatchEventKinds.ENTRY_MODIFY;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonMap;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.commons.io.FileUtils.readFileToString;
import static org.apache.commons.io.FileUtils.writeStringToFile;
import static org.apache.commons.lang.StringUtils.isNotBlank;
import static org.apache.commons.lang.StringUtils.reverse;
import static org.apache.commons.lang.StringUtils.substringBefore;
import static org.apache.commons.lang3.RandomStringUtils.randomAlphanumeric;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeThat;
import static org.mule.rx.RxMule.asAction;
import static org.mule.rx.RxMule.asFunc;
import static org.mule.rx.RxMule.respondSync;
import static org.mule.transformer.types.DataTypeFactory.OBJECT;
import static org.mule.transformer.types.DataTypeFactory.STRING;
import static org.mule.transport.http.HttpConnector.HTTP_METHOD_PROPERTY;
import static org.mule.transport.http.HttpConnector.HTTP_STATUS_PROPERTY;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URI;
import java.net.URL;
import java.nio.file.FileSystems;
import java.nio.file.Paths;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.List;
import java.util.concurrent.Exchanger;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import javax.resource.spi.IllegalStateException;

import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mule.api.MuleEvent;
import org.mule.api.MuleException;
import org.mule.api.MuleMessage;
import org.mule.api.callback.SourceCallback;
import org.mule.api.client.MuleClient;
import org.mule.api.source.MessageSource;
import org.mule.construct.Flow;
import org.mule.module.http.api.listener.HttpListenerBuilder;
import org.mule.module.redis.RedisModule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import rx.Observable;
import rx.Subscription;
import rx.functions.Action1;
import rx.functions.Func1;

public class RxMuleITCase
{
    private static final Logger LOGGER = LoggerFactory.getLogger(RxMuleITCase.class);

    private static final String TEST_HTTP_PATH = "/rx-mule";

    @Rule
    public TemporaryFolder inputFolder = new TemporaryFolder();

    @Rule
    public TemporaryFolder outputFolder = new TemporaryFolder();

    private WatchService outputFolderWatcher;

    @BeforeClass
    public static void globalConfig()
    {
        System.setProperty("mule.verbose.exceptions", "true");
    }

    @Before
    public void initializeWatchService() throws IOException
    {
        outputFolderWatcher = FileSystems.getDefault().newWatchService();
        Paths.get(outputFolder.getRoot().toURI()).register(outputFolderWatcher, ENTRY_MODIFY);
    }

    @Test
    public void singleAsyncEndpoint() throws Exception
    {
        final URI inboundHttpUri = new URI("http://localhost:" + freePort() + TEST_HTTP_PATH);
        final URI outboundFileUri = outputFolder.getRoot().toURI();

        try (final RxMule rxMule = RxMule.create())
        {
            // defines an asynchronous HTTP -> File bridge, only handling POST requests
            rxMule.observeEndpointAsync(inboundHttpUri).filter(new Func1<MuleEvent, Boolean>()
            {
                @Override
                public Boolean call(final MuleEvent muleEvent)
                {
                    return "POST".equals(muleEvent.getMessage().getInboundProperty(HTTP_METHOD_PROPERTY));
                }
            }).subscribe(asAction(rxMule.dispatchAsync(outboundFileUri)));

            // test a GET request
            given().port(inboundHttpUri.getPort())
                .get(TEST_HTTP_PATH)
                .then()
                .assertThat()
                .statusCode(200)
                .and()
                .header("Content-Length", "0");

            // test a POST request
            final String testData = randomAlphanumeric(100);

            given().port(inboundHttpUri.getPort())
                .and()
                .body(testData)
                .post(TEST_HTTP_PATH)
                .then()
                .assertThat()
                .statusCode(200)
                .and()
                .header("Content-Length", "0");

            final File outputFile = waitFileCreatedInOutputFolder();
            assertThat(readFileToString(outputFile), is(testData));
        }
    }

    @Test
    public void compositeEndpoint() throws Exception
    {
        final URI inboundHttpUri = new URI("http://localhost:" + freePort() + TEST_HTTP_PATH);
        final URI inboundFileUri = inputFolder.getRoot().toURI();
        final URI outboundFileUri = outputFolder.getRoot().toURI();

        try (final RxMule rxMule = RxMule.create())
        {
            // defines an composite synchronous HTTP / async File source -> File bridge, only
            // handling POST requests
            final Subscription subscription = Observable.merge(rxMule.observeEndpointAsync(inboundHttpUri),
                rxMule.observeEndpointAsync(inboundFileUri))
                .filter(new Func1<MuleEvent, Boolean>()
                {
                    @Override
                    public Boolean call(final MuleEvent muleEvent)
                    {
                        // accept any event coming from the file inbound endpoint and only HTTP POST
                        // requests from the HTTP inbound endpoint.
                        return "file".equals(muleEvent.getMessageSourceURI().getScheme())
                               || "POST".equals(muleEvent.getMessage().getInboundProperty(
                                   HTTP_METHOD_PROPERTY));
                    }
                })
                .subscribe(asAction(rxMule.dispatchAsync(outboundFileUri)));

            // test a POST request
            String testData = RandomStringUtils.randomAlphanumeric(100);

            given().port(inboundHttpUri.getPort())
                .and()
                .body(testData)
                .post(TEST_HTTP_PATH)
                .then()
                .assertThat()
                .statusCode(200)
                .and()
                .header("Content-Length", "0");

            File outputFile = waitFileCreatedInOutputFolder();
            assertThat(readFileToString(outputFile), is(testData));

            // test a FILE drop
            testData = randomAlphanumeric(100);
            writeStringToFile(inputFolder.newFile(), testData);
            outputFile = waitFileCreatedInOutputFolder();
            assertThat(readFileToString(outputFile), is(testData));

            // checking that unsubscription works
            subscription.unsubscribe();

            assertThat("Port was not correctly closed after unsubscription from endpoint",
                isPortFree(inboundHttpUri.getPort()), is(true));
        }
    }

    @Test
    public void singleSyncEndpoint() throws Exception
    {
        final URI inboundHttpUri = new URI("http://localhost:" + freePort() + TEST_HTTP_PATH);

        try (final RxMule rxMule = RxMule.create())
        {
            // defines an synchronous HTTP endpoint
            rxMule.observeEndpointSync(inboundHttpUri)
            // the following transformer is not necessary since we use
            // muleEvent.getMessageAsString() below but it demonstrates how to use a transformer
                .map(asFunc(rxMule.getTransformer(OBJECT, STRING)))
                .subscribe(new RxMuleAction1()
                {
                    @Override
                    protected void doCall(final MuleEvent muleEvent) throws MuleException
                    {
                        // the source is a request-respons HTTP inbound endpoint so no need to call
                        // RxMule.canRespondSync() because we know the event is synchronous
                        respondSync(reverse(muleEvent.getMessageAsString()), muleEvent);
                    }
                });

            // test a POST request
            final String testData = randomAlphanumeric(100);

            given().port(inboundHttpUri.getPort())
                .and()
                .body(testData)
                .post(TEST_HTTP_PATH)
                .then()
                .assertThat()
                .statusCode(200)
                .and()
                .body(is(reverse(testData)));
        }
    }

    @Test
    public void errorHandling() throws Exception
    {
        final URI inboundHttpUri = new URI("http://localhost:" + freePort() + TEST_HTTP_PATH);

        try (final RxMule rxMule = RxMule.create())
        {
            rxMule.observeEndpointSync(inboundHttpUri).subscribe(new RxMuleAction1()
            {
                @Override
                protected void doCall(final MuleEvent muleEvent) throws MuleException
                {
                    throw new RuntimeException("simulated problem for: " + muleEvent.getMessageAsString());
                }
            }, new Action1<Throwable>()
            {
                @Override
                public void call(final Throwable t)
                {
                    final MuleEvent failedEvent = ((RxMuleEventException) t).getMuleEvent();

                    // the source is a request-respons HTTP inbound endpoint so no need to call
                    // RxMule.canRespondSync() because we know the event is synchronous
                    respondSync(t.getCause().getMessage(), singletonMap(HTTP_STATUS_PROPERTY, 503),
                        failedEvent);
                }
            });

            final String testData = randomAlphanumeric(100);

            given().port(inboundHttpUri.getPort())
                .and()
                .body(testData)
                .post(TEST_HTTP_PATH)
                .then()
                .assertThat()
                .statusCode(503)
                .and()
                .body(is("simulated problem for: " + testData));
        }
    }

    @Test
    public void synchronousRequestTimeOut() throws Exception
    {
        final URI inboundHttpUri = new URI("http://localhost:" + freePort() + TEST_HTTP_PATH
                                           + "?responseTimeout=2000");

        try (final RxMule rxMule = RxMule.create())
        {
            rxMule.observeEndpointSync(inboundHttpUri).subscribe(new RxMuleAction1()
            {
                @Override
                protected void doCall(final MuleEvent muleEvent) throws MuleException
                {
                    // ooops, forgot to call respondSync
                }
            });

            given().port(inboundHttpUri.getPort())
                .and()
                .get(TEST_HTTP_PATH)
                .then()
                .assertThat()
                .statusCode(500)
                .and()
                .body(startsWith("Failed"));
        }
    }

    @Test
    public void messageSource() throws Exception
    {
        final URL inboundHttpUrl = new URL("http://localhost:" + freePort() + TEST_HTTP_PATH);

        try (final RxMule rxMule = RxMule.create())
        {
            rxMule.observeMessageSource(new RxMessageSourceProvider()
            {
                @Override
                public MessageSource doCall(final Flow flow) throws MuleException
                {
                    return new HttpListenerBuilder(rxMule.getMuleContext()).setUrl(inboundHttpUrl)
                        .setFlow(flow)
                        .setSuccessStatusCode("201")
                        .build();
                }
            }).subscribe(new RxMuleAction1()
            {
                @Override
                public void doCall(final MuleEvent muleEvent) throws MuleException
                {
                    respondSync(reverse(muleEvent.getMessageAsString()), muleEvent);
                }
            });

            // test a POST request
            final String testData = randomAlphanumeric(100);

            given().port(inboundHttpUrl.getPort())
                .and()
                .body(testData)
                .post(TEST_HTTP_PATH)
                .then()
                .assertThat()
                .statusCode(201)
                .and()
                .body(is(reverse(testData)));
        }
    }

    @Test
    public void globalEndpoint() throws Exception
    {
        try (final RxMule rxMule = RxMule.create("mule-global-endpoint-config.xml"))
        {
            // a global endpoint -> global endpoint bridge
            rxMule.observeGlobalEndpoint("global-endpoint-1").subscribe(
                asAction(rxMule.globalEndpoint("global-endpoint-2")));

            final String testData = randomAlphanumeric(100);

            final MuleClient muleClient = rxMule.getMuleContext().getClient();
            muleClient.dispatch("vm://rxmule.test.global.1", rxMule.newMuleMessage(testData));
            final MuleMessage message = muleClient.request("vm://rxmule.test.global.2", 10000L);

            assertThat(message.getPayloadAsString(), is(testData));
        }
    }

    @Test
    public void anypointMessageSource() throws Exception
    {
        assumeThat("Assume Redis is accessible at localhost:6379", isPortFree(6379), is(false));

        final String channel = "rx-mule.test." + randomAlphanumeric(100);
        final Exchanger<String> testPayloadExchanger = new Exchanger<>();

        try (final RxMule rxMule = RxMule.create("mule-redis-config.xml"))
        {
            final RedisModule redisModule = rxMule.getAnypointConnector("localRedis");

            rxMule.observeAnypointSource(redisModule, new Action1<SourceCallback>()
            {
                @Override
                public void call(final SourceCallback sourceCallback)
                {
                    redisModule.subscribe(asList(channel), sourceCallback);
                }
            }).subscribe(new Action1<MuleEvent>()
            {
                @Override
                public void call(final MuleEvent muleEvent)
                {
                    try
                    {
                        testPayloadExchanger.exchange(muleEvent.getMessageAsString(), 30L, SECONDS);
                    }
                    catch (final Exception e)
                    {
                        throw new RuntimeException(e);
                    }
                }
            });

            waitForRedisSubscriber();

            final String testData = "message for " + channel;
            redisModule.getJedisPool().getResource().publish(channel, testData);

            final String actualPayload = testPayloadExchanger.exchange(null, 30L, SECONDS);
            assertThat(actualPayload, is(testData));
        }
    }

    @Test
    public void anypointMessageProcessor() throws Exception
    {
        assumeThat("Assume Redis is accessible at localhost:6379", isPortFree(6379), is(false));

        final URI inboundHttpUri = new URI("http://localhost:" + freePort() + TEST_HTTP_PATH);
        final String incKey = "rx-mule.test." + randomAlphanumeric(100);

        try (final RxMule rxMule = RxMule.create("mule-redis-config.xml"))
        {
            final RedisModule redisModule = rxMule.getAnypointConnector("localRedis");

            // defines an synchronous HTTP endpoint
            rxMule.observeEndpointSync(inboundHttpUri).subscribe(new RxMuleAction1()
            {
                @Override
                protected void doCall(final MuleEvent muleEvent) throws MuleException
                {
                    redisModule.increment(incKey, 1L);

                    respondSync(incKey, muleEvent);
                }
            });

            given().port(inboundHttpUri.getPort())
                .get(TEST_HTTP_PATH)
                .then()
                .assertThat()
                .statusCode(200)
                .and()
                .body(is(incKey));

            assertThat(redisModule.get(incKey), is("1".getBytes()));
        }
    }

    @Test
    public void distinct() throws Exception
    {
        assumeThat("Assume Redis is accessible at localhost:6379", isPortFree(6379), is(false));

        final URI inboundHttpUri = new URI("http://localhost:" + freePort() + TEST_HTTP_PATH);
        final AtomicInteger counter = new AtomicInteger(0);

        try (final RxMule rxMule = RxMule.create("mule-redis-config.xml"))
        {
            final RedisModule redisModule = rxMule.getAnypointConnector("localRedis");

            rxMule.observeEndpointAsync(inboundHttpUri).distinct(new Func1<MuleEvent, String>()
            {
                @Override
                public String call(final MuleEvent muleEvent)
                {
                    final String remoteAddressAndPort = muleEvent.getMessage().getInboundProperty(
                        "MULE_REMOTE_CLIENT_ADDRESS");

                    return substringBefore(remoteAddressAndPort, ":");
                }
            })
                .subscribe(new RxMuleAction1()
                {
                    @Override
                    protected void doCall(final MuleEvent muleEvent) throws MuleException
                    {
                        redisModule.publish("http-requests", false, muleEvent.getMessageAsString(), muleEvent);

                        LOGGER.info("Published: {}", muleEvent);

                        counter.incrementAndGet();
                    }
                });

            for (int i = 0; i < 5; i++)
            {
                given().port(inboundHttpUri.getPort())
                    .get(TEST_HTTP_PATH)
                    .then()
                    .assertThat()
                    .statusCode(200);
            }

            assertThat(counter.get(), is(1));
        }
    }

    private File waitFileCreatedInOutputFolder() throws InterruptedException
    {
        final File created = waitFileCreatedInOutputFolder(30L, SECONDS);
        if (created == null)
        {
            fail("No file written in output folder, timing out!");
        }

        // the file may still be writing
        while (waitFileCreatedInOutputFolder(100, MILLISECONDS) != null);

        return created;
    }

    private File waitFileCreatedInOutputFolder(final long timeOut, final TimeUnit unit)
        throws InterruptedException
    {
        final WatchKey watchKey = outputFolderWatcher.poll(timeOut, unit);
        if (watchKey == null)
        {
            return null;
        }

        final List<WatchEvent<?>> events = watchKey.pollEvents();
        assertThat(events, hasSize(1));

        watchKey.reset();

        return new File(outputFolder.getRoot(), events.get(0).context().toString());
    }

    private static int freePort() throws IOException
    {
        // yes, this is prone to race conditions (another process can grab the port after it's been
        // tested) but it's good enough for most situations, since the port selection is randomized
        try (ServerSocket ss = new ServerSocket(0))
        {
            return ss.getLocalPort();
        }
    }

    private static boolean isPortFree(final int port)
    {
        try (Socket _ = new Socket("localhost", port))
        {
            return false;
        }
        catch (final IOException _)
        {
            return true;
        }
    }

    private static void waitForRedisSubscriber() throws Exception
    {
        for (int i = 0; i < 30; i++)
        {
            if (hasRedisSubscriber())
            {
                return;
            }

            Thread.sleep(500L);
        }

        throw new IllegalStateException("Never had a Redis subscriber");
    }

    private static boolean hasRedisSubscriber() throws Exception
    {
        try (final Socket redisSocket = new Socket("localhost", 6379);
                        final PrintWriter out = new PrintWriter(redisSocket.getOutputStream(), true);
                        final BufferedReader in = new BufferedReader(new InputStreamReader(
                            redisSocket.getInputStream())))
        {
            out.println("CLIENT LIST");

            String line;
            while (isNotBlank(line = in.readLine()))
            {
                if (line.contains("psub=1"))
                {
                    return true;
                }
            }
        }

        return false;
    }
}
