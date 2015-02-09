
package org.mule.rx.support;

import static java.util.Collections.singletonList;
import static org.mule.api.MuleEvent.TIMEOUT_NOT_SET_VALUE;

import java.io.Closeable;
import java.io.IOException;

import org.apache.commons.lang.Validate;
import org.mule.api.MuleEvent;
import org.mule.api.MuleException;
import org.mule.api.construct.FlowConstructAware;
import org.mule.api.context.MuleContextAware;
import org.mule.api.processor.MessageProcessor;
import org.mule.api.source.MessageSource;
import org.mule.construct.Flow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import rx.Observable.OnSubscribe;
import rx.Subscriber;
import rx.functions.Func1;

public class ObservableMessageSource implements OnSubscribe<MuleEvent>, Closeable
{
    private static final Logger LOGGER = LoggerFactory.getLogger(ObservableMessageSource.class);

    private final Func1<Flow, MessageSource> messageSourceBuilder;
    private final WorkManagerCache workManagerCache;

    private volatile Flow flow;

    public ObservableMessageSource(final Func1<Flow, MessageSource> messageSourceBuilder,
                                   final WorkManagerCache workManagerCache)
    {
        Validate.notNull(messageSourceBuilder, "messageSourceBuilder can't be null");
        Validate.notNull(workManagerCache, "workManagerCache can't be null");

        this.messageSourceBuilder = messageSourceBuilder;
        this.workManagerCache = workManagerCache;
    }

    @Override
    public void call(final Subscriber<? super MuleEvent> subscriber)
    {
        flow = new SubscriberFlow(subscriber, "RxMule-MSB-" + messageSourceBuilder.hashCode() + "-"
                                              + subscriber.hashCode(), workManagerCache.getMuleContext());

        final MessageSource messageSource = messageSourceBuilder.call(flow);

        if (messageSource instanceof MuleContextAware)
        {
            ((MuleContextAware) messageSource).setMuleContext(workManagerCache.getMuleContext());
        }

        if (messageSource instanceof FlowConstructAware)
        {
            ((FlowConstructAware) messageSource).setFlowConstruct(flow);
        }

        flow.setMessageSource(messageSource);

        flow.setMessageProcessors(singletonList((MessageProcessor) new MessageProcessorSubscriberListener(
            subscriber, TIMEOUT_NOT_SET_VALUE, workManagerCache.getReceiverWorkManager(messageSource))));

        try
        {
            flow.getMuleContext().getRegistry().registerFlowConstruct(flow);
            LOGGER.debug("Subscription started flow: {}", flow);
        }
        catch (final MuleException me)
        {
            throw new RuntimeException("Failed to start flow: " + flow, me);
        }
    }

    @Override
    public void close() throws IOException
    {
        if (flow == null)
        {
            return;
        }

        try
        {
            flow.getMuleContext().getRegistry().unregisterFlowConstruct(flow.getName());
        }
        catch (final MuleException me)
        {
            throw new IOException("Failed to unregister flow: " + flow.getName());
        }
    }
}
