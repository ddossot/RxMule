
package org.mule.rx;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.mule.MessageExchangePattern.ONE_WAY;
import static org.mule.MessageExchangePattern.REQUEST_RESPONSE;
import static org.mule.api.client.SimpleOptionsBuilder.newOptions;

import java.io.Closeable;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Exchanger;
import java.util.concurrent.TimeUnit;

import javax.activation.DataHandler;

import org.apache.commons.lang.Validate;
import org.mule.DefaultMuleEvent;
import org.mule.DefaultMuleMessage;
import org.mule.MessageExchangePattern;
import org.mule.VoidMuleEvent;
import org.mule.api.MuleContext;
import org.mule.api.MuleEvent;
import org.mule.api.MuleException;
import org.mule.api.MuleMessage;
import org.mule.api.callback.SourceCallback;
import org.mule.api.config.ConfigurationBuilder;
import org.mule.api.context.MuleContextBuilder;
import org.mule.api.context.MuleContextFactory;
import org.mule.api.endpoint.EndpointBuilder;
import org.mule.api.endpoint.InboundEndpoint;
import org.mule.api.processor.MessageProcessor;
import org.mule.api.source.MessageSource;
import org.mule.api.transformer.DataType;
import org.mule.api.transformer.Transformer;
import org.mule.api.transformer.TransformerException;
import org.mule.config.AnnotationsConfigurationBuilder;
import org.mule.config.spring.SpringXmlConfigurationBuilder;
import org.mule.construct.Flow;
import org.mule.context.DefaultMuleContextBuilder;
import org.mule.context.DefaultMuleContextFactory;
import org.mule.rx.support.CloseableEndpointCache;
import org.mule.rx.support.Closeables;
import org.mule.rx.support.ObservableMessageSource;
import org.mule.rx.support.SourceCallbackMessageSourceAdapter;
import org.mule.rx.support.WorkManagerCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import rx.Observable;
import rx.functions.Action0;
import rx.functions.Action1;
import rx.functions.Func1;

/**
 * Reactive Extensions bindings for <a
 * href="http://www.mulesoft.com/platform/soa/mule-esb-open-source-esb">Mule ESB</a>.
 */
public final class RxMule implements Closeable
{
    /**
     * The missing member of the {@link MessageSource}, {@link MessageProcessor} and
     * {@link MessageConsumer} functional interface trinity.
     */
    public interface MessageConsumer
    {
        void consume(MuleEvent muleEvent) throws MuleException;
    }

    private final class UnsubscribeAction0 implements Action0
    {
        private final Closeable closeable;

        private UnsubscribeAction0(final Closeable closeable)
        {
            Validate.notNull(closeable, "closeable can't be null");
            this.closeable = closeable;
        }

        @Override
        public void call()
        {
            try
            {
                closeables.stopAndDeregister(closeable);

                LOGGER.debug("Subscription stopped: {}", closeable);
            }
            catch (final IOException ioe)
            {
                throw new RuntimeException("Failed to unsubscribe from: " + closeable, ioe);
            }
        }
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(RxMule.class);

    public static final String RX_MULE_RESPONSE_EXCHANGER = "rx-mule.response-exchanger";

    private final MuleContext muleContext;
    private final boolean owningMuleContext;
    private final CloseableEndpointCache endpointCache;
    private final WorkManagerCache workManagerCache;
    private final Closeables closeables;

    private RxMule(final MuleContext muleContext, final boolean owningMuleContext)
    {
        Validate.notNull(muleContext, "muleContext can't be null");

        this.muleContext = muleContext;
        this.owningMuleContext = owningMuleContext;

        this.endpointCache = new CloseableEndpointCache(muleContext);
        this.workManagerCache = new WorkManagerCache(muleContext);
        this.closeables = new Closeables();

        LOGGER.debug("Initialized.");
    }

    @Override
    public void close() throws IOException
    {
        LOGGER.debug("Closing. Owning context? {}", owningMuleContext);

        closeables.close();
        workManagerCache.close();
        endpointCache.close();

        if (owningMuleContext)
        {
            muleContext.dispose();
        }
    }

    /**
     * Creates an {@link RxMule} wrapper around an existing {@link MuleContext}.
     */
    public static RxMule create(final MuleContext muleContext)
    {
        return new RxMule(muleContext, false);
    }

    /**
     * Creates a new {@link RxMule} instance, optionally loading a list of Mule XML configuration
     * files. XML configuration files are where you would put your connectors configurations and
     * global endpoints.
     */
    public static RxMule create(final String... configResources) throws MuleException
    {
        final List<ConfigurationBuilder> configurationBuilders = new ArrayList<ConfigurationBuilder>();
        configurationBuilders.add(new AnnotationsConfigurationBuilder());
        configurationBuilders.add(new SpringXmlConfigurationBuilder(configResources));

        final MuleContextBuilder muleContextBuilder = new DefaultMuleContextBuilder();
        final MuleContextFactory muleContextFactory = new DefaultMuleContextFactory();
        final MuleContext muleContext = muleContextFactory.createMuleContext(configurationBuilders,
            muleContextBuilder);

        muleContext.start();

        return new RxMule(muleContext, true);
    }

    public MuleContext getMuleContext()
    {
        return muleContext;
    }

    /**
     * Retrieves an Anypoint connector (aka Module) configured in XML, by its global name, or fail.
     */
    public <T> T getAnypointConnector(final String name)
    {
        final T connector = muleContext.getRegistry().lookupObject(name);

        if (connector == null)
        {
            throw new IllegalArgumentException("No Anypoint Connector found in registry with name: " + name);
        }

        return connector;
    }

    /**
     * Retrieves a {@link Transformer} instance or fails.
     */
    public Transformer getTransformer(final DataType<?> source, final DataType<?> result)
    {
        try
        {
            return muleContext.getRegistry().lookupTransformer(source, result);
        }
        catch (final TransformerException e)
        {
            throw new IllegalArgumentException("No transformer found in registry for: " + source + " -> "
                                               + result);
        }
    }

    /**
     * Creates an {@link Observable} from an asynchronous (aka one-way) {@link InboundEndpoint}
     * defined by the provided {@link URI}. The inbound endpoint starts when the observable gets
     * subscribed to and stops when it gets unsubscribed from.
     */
    public Observable<MuleEvent> observeEndpointAsync(final URI uri) throws MuleException
    {
        return observeEndpoint(uri, ONE_WAY);
    }

    /**
     * Creates an {@link Observable} from a synchronous (aka request-response)
     * {@link InboundEndpoint} defined by the provided {@link URI}. The inbound endpoint starts when
     * the observable gets subscribed to and stops when it gets unsubscribed from.
     */
    public Observable<MuleEvent> observeEndpointSync(final URI uri) throws MuleException
    {
        return observeEndpoint(uri, REQUEST_RESPONSE);
    }

    /**
     * Creates an {@link Observable} from a global endpoint referred to by the provided name. The
     * generated inbound endpoint starts when the observable gets subscribed to and stops when it
     * gets unsubscribed from.
     */
    public Observable<MuleEvent> observeGlobalEndpoint(final String name) throws MuleException
    {
        return observeEndpoint(getGlobalEndpointOrBail(name).buildInboundEndpoint());
    }

    private EndpointBuilder getGlobalEndpointOrBail(final String name)
    {
        final EndpointBuilder endpointBuilder = muleContext.getRegistry().lookupEndpointBuilder(name);

        if (endpointBuilder == null)
        {
            throw new IllegalArgumentException("No global endpoint named: " + name
                                               + " can be found in registry.");
        }
        return endpointBuilder;
    }

    private Observable<MuleEvent> observeEndpoint(final URI uri, final MessageExchangePattern mep)
        throws MuleException
    {
        return observeEndpoint(endpointCache.getInboundEndpoint(uri.toString(), mep));
    }

    private Observable<MuleEvent> observeEndpoint(final InboundEndpoint ie)
    {
        return observeMessageSource(new Func1<Flow, MessageSource>()
        {
            @Override
            public MessageSource call(final Flow flow)
            {
                ie.setFlowConstruct(flow);
                return ie;
            }
        });
    }

    /**
     * Creates an {@link Observable} from an Anypoint connector method that accepts a
     * {@link SourceCallback}.
     */
    public Observable<MuleEvent> observeAnypointSource(final Object connector,
                                                       final Action1<SourceCallback> sourceCallbackConsumer)
    {
        return observeMessageSource(new Func1<Flow, MessageSource>()
        {
            @Override
            public MessageSource call(final Flow flow)
            {
                return new SourceCallbackMessageSourceAdapter(sourceCallbackConsumer, connector);
            }
        });
    }

    /**
     * Creates an {@link Observable} from a message source that needs to be configured with a
     * {@link Flow} instance.
     */
    public Observable<MuleEvent> observeMessageSource(final Func1<Flow, MessageSource> messageSourceBuilder)
    {
        final ObservableMessageSource oms = new ObservableMessageSource(messageSourceBuilder,
            workManagerCache);

        closeables.register(oms);

        return Observable.create(oms).doOnUnsubscribe(new UnsubscribeAction0(oms));
    }

    /**
     * Creates a {@link MessageProcessor} that can dispatch asynchronously the current event to the
     * provided {@link URI}. It can be used with {@link Observable} methods via
     * {@link RxMule#asFunc(MessageProcessor)} and {@link RxMule#asAction(MessageProcessor)}.
     */
    public MessageProcessor dispatchAsync(final URI uri)
    {
        return new MessageProcessor()
        {
            @Override
            public MuleEvent process(final MuleEvent muleEvent) throws MuleException
            {
                muleContext.getClient().dispatch(uri.toString(), muleEvent.getMessage());

                return muleEvent;
            }
        };
    }

    /**
     * Creates a {@link MessageProcessor} that can send synchronously the current event to the
     * provided {@link URI}, waiting for a response for the specified time-out. It can be used with
     * {@link Observable} methods via {@link RxMule#asFunc(MessageProcessor)} and
     * {@link RxMule#asAction(MessageProcessor)}.
     */
    public MessageProcessor sendSync(final URI uri, final long responseTimeout, final TimeUnit timeUnit)
    {
        return new MessageProcessor()
        {
            @Override
            public MuleEvent process(final MuleEvent muleEvent) throws MuleException
            {
                final MuleMessage responseMessage = muleContext.getClient().send(uri.toString(),
                    muleEvent.getMessage(),
                    newOptions().responseTimeout(MILLISECONDS.convert(responseTimeout, timeUnit)).build());

                return new DefaultMuleEvent(responseMessage, muleEvent);
            }
        };
    }

    /**
     * Creates a {@link MessageProcessor} from a global endpoint, which can either send
     * synchronously or dispatch asynchronously the current event (depending on the configuration of
     * the global endpoint). It can be used with {@link Observable} methods via
     * {@link RxMule#asFunc(MessageProcessor)} and {@link RxMule#asAction(MessageProcessor)}.
     */
    public MessageProcessor globalEndpoint(final String name) throws MuleException
    {
        return getGlobalEndpointOrBail(name).buildOutboundEndpoint();
    }

    /**
     * Adapts a {@link MessageProcessor} instance into a <code>Func1&lt;MuleEvent, MuleEvent></code>
     * (see {@link Func1}), which can be used in many of the {@link Observable} methods.
     */
    public static Func1<MuleEvent, MuleEvent> asFunc(final MessageProcessor messageProcessor)
    {
        Validate.notNull(messageProcessor, "messageProcessor can't be null");

        return new RxMuleFunc1<MuleEvent>()
        {
            @Override
            protected MuleEvent doCall(final MuleEvent muleEvent) throws MuleException
            {
                final MuleEvent resultEvent = messageProcessor.process(muleEvent);

                // return the source event, making this processor a no-op
                return resultEvent instanceof VoidMuleEvent ? muleEvent : resultEvent;
            }
        };
    }

    /**
     * Adapts a {@link MessageProcessor} instance into a <code>Action1&lt;MuleEvent></code> (see
     * {@link Action1}), which can be used in many of the {@link Observable} methods.
     */
    public static Action1<MuleEvent> asAction(final MessageProcessor messageProcessor)
    {
        Validate.notNull(messageProcessor, "messageProcessor can't be null");

        return new RxMuleAction1()
        {
            @Override
            protected void doCall(final MuleEvent muleEvent) throws MuleException
            {
                messageProcessor.process(muleEvent);
            }
        };
    }

    /**
     * Adapts a {@link MessageProcessor} instance into a <code>Action1&lt;MuleEvent></code> (see
     * {@link Action1}), which can be used in many of the {@link Observable} methods.
     */
    public static Action1<MuleEvent> asAction(final MessageConsumer messageConsumer)
    {
        Validate.notNull(messageConsumer, "messageConsumer can't be null");

        return new RxMuleAction1()
        {
            @Override
            protected void doCall(final MuleEvent muleEvent) throws MuleException
            {
                messageConsumer.consume(muleEvent);
            }
        };
    }

    /**
     * Responds to an inbound {@link MuleEvent}, in case the source event is synchronous (aka
     * request-response), <b>which can be done only once for the lifetime of an event</b>. It is the
     * caller's responsibility to use this method only when a synchronous response can be routed
     * back to a caller thread. Using {@link RxMule#canRespondSync(MuleEvent)} can help finding out
     * if it is safe to call this method.
     *
     * @throws RxMuleEventException if the event is not able to route a synchronous response.
     */
    public static void respondSync(final Object payload, final MuleEvent requestEvent)
        throws RxMuleEventException
    {
        respondSync(payload, null, requestEvent);
    }

    /**
     * Responds to an inbound {@link MuleEvent}, in case the source event is synchronous (aka
     * request-response), <b>which can be done only once for the lifetime of an event</b>. It is the
     * caller's responsibility to use this method only when a synchronous response can be routed
     * back to a caller thread. Using {@link RxMule#canRespondSync(MuleEvent)} can help finding out
     * if it is safe to call this method.
     *
     * @throws RxMuleEventException if the event is not able to route a synchronous response.
     */
    public static void respondSync(final Object payload,
                                   final Map<String, ? extends Object> properties,
                                   final MuleEvent muleEvent) throws RxMuleEventException
    {
        Validate.notNull(muleEvent, "muleEvent can't be null");

        @SuppressWarnings("unchecked")
        final MuleEvent responseEvent = new DefaultMuleEvent(new DefaultMuleMessage(payload,
            (Map<String, Object>) properties, muleEvent.getMuleContext()), muleEvent);

        synchronized (muleEvent)
        {
            @SuppressWarnings("unchecked")
            final Exchanger<MuleEvent> responseExchanger = (Exchanger<MuleEvent>) muleEvent.getFlowVariable(RX_MULE_RESPONSE_EXCHANGER);

            muleEvent.removeFlowVariable(RX_MULE_RESPONSE_EXCHANGER);

            if (responseExchanger == null)
            {
                throw new RxMuleEventException(
                    "No response exchanger found in MuleEvent, can't route synchrounous response event: "
                                    + responseEvent, muleEvent);
            }

            try
            {

                responseExchanger.exchange(responseEvent, muleEvent.getTimeout(), MILLISECONDS);
            }
            catch (final Exception e)
            {
                throw new RxMuleEventException("Failed to route synchrounous response event: "
                                               + responseEvent, muleEvent, e);
            }
        }
    }

    /**
     * Checks if the provided {@link MuleEvent} is able to route a synchronous response.
     */
    public static boolean canRespondSync(final MuleEvent muleEvent)
    {
        return muleEvent.getFlowVariableNames().contains(RX_MULE_RESPONSE_EXCHANGER);
    }

    /**
     * Helper for creating a new {@link MuleMessage} instance, with no properties.
     */
    public MuleMessage newMuleMessage(final Object payload)
    {
        return new DefaultMuleMessage(payload, muleContext);
    }

    /**
     * Helper for creating a new {@link MuleMessage} instance, with outbound properties.
     */
    public MuleMessage newMuleMessage(final Object payload, final Map<String, Object> outboundProperties)
    {
        return new DefaultMuleMessage(payload, outboundProperties, muleContext);
    }

    /**
     * Helper for creating a new {@link MuleMessage} instance, with outbound properties and
     * attachments.
     */
    public MuleMessage newMuleMessage(final Object payload,
                                      final Map<String, Object> outboundProperties,
                                      final Map<String, DataHandler> attachments)
    {
        return new DefaultMuleMessage(payload, outboundProperties, attachments, muleContext);
    }

    /**
     * Helper for creating a new {@link MuleMessage} instance, with inbound and outbound properties,
     * and attachments.
     */
    public MuleMessage newMuleMessage(final Object payload,
                                      final Map<String, Object> inboundProperties,
                                      final Map<String, Object> outboundProperties,
                                      final Map<String, DataHandler> attachments)
    {
        return new DefaultMuleMessage(payload, inboundProperties, outboundProperties, attachments,
            muleContext);
    }
}
