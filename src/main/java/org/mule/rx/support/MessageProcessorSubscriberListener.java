
package org.mule.rx.support;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.mule.api.MuleEvent.TIMEOUT_NOT_SET_VALUE;
import static org.mule.config.i18n.MessageFactory.createStaticMessage;
import static org.mule.rx.RxMule.RX_MULE_RESPONSE_EXCHANGER;
import static org.mule.rx.support.Subscribers.routeEventToSubscriber;

import java.util.Map;
import java.util.concurrent.Exchanger;

import org.apache.commons.lang.Validate;
import org.mule.DefaultMuleEvent;
import org.mule.DefaultMuleMessage;
import org.mule.api.MessagingException;
import org.mule.api.MuleEvent;
import org.mule.api.MuleException;
import org.mule.api.context.WorkManager;
import org.mule.api.lifecycle.Stoppable;
import org.mule.api.processor.MessageProcessor;
import org.mule.message.DefaultExceptionPayload;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import rx.Subscriber;

public class MessageProcessorSubscriberListener implements MessageProcessor, Stoppable
{
    private static final Logger LOGGER = LoggerFactory.getLogger(MessageProcessorSubscriberListener.class);

    private final Subscriber<? super MuleEvent> subscriber;
    private final int timeOutMillis;
    private final WorkManager workManager;

    public MessageProcessorSubscriberListener(final Subscriber<? super MuleEvent> subscriber,
                                              final int timeOutMillis,
                                              final WorkManager workManager)
    {
        Validate.notNull(subscriber, "subscriber can't be null");
        Validate.notNull(workManager, "workManager can't be null");

        this.subscriber = subscriber;
        this.timeOutMillis = timeOutMillis;
        this.workManager = workManager;
    }

    @Override
    public void stop() throws MuleException
    {
        subscriber.onCompleted();
    }

    @Override
    public MuleEvent process(final MuleEvent muleEvent) throws MuleException
    {
        if (timeOutMillis != TIMEOUT_NOT_SET_VALUE && muleEvent.getTimeout() != timeOutMillis)
        {
            muleEvent.setTimeout(timeOutMillis);
        }

        if (muleEvent.isSynchronous())
        {
            final Exchanger<MuleEvent> responseExchanger = new Exchanger<>();

            muleEvent.setFlowVariable(RX_MULE_RESPONSE_EXCHANGER, responseExchanger);

            routeEventToSubscriber(muleEvent, subscriber, workManager);

            try
            {
                LOGGER.debug("Waiting for a synchronous response for: " + muleEvent.getTimeout() + "ms");

                return responseExchanger.exchange(null, muleEvent.getTimeout(), MILLISECONDS);
            }
            catch (final Throwable t)
            {
                LOGGER.error(
                    "Failed to receive a synchronous response for event: {}, will return a failed event.",
                    muleEvent);

                return newFailureResponseEvent(
                    "Failed to receive a synchronous response after: " + muleEvent.getTimeout() + " ms",
                    null, t, muleEvent);
            }
        }
        else
        {
            routeEventToSubscriber(muleEvent, subscriber, workManager);
            return null;
        }
    }

    private static MuleEvent newFailureResponseEvent(final String message,
                                                     final Map<String, ? extends Object> properties,
                                                     final Throwable t,
                                                     final MuleEvent muleEvent)
    {
        @SuppressWarnings("unchecked")
        final DefaultMuleMessage responseMessage = new DefaultMuleMessage(message,
            (Map<String, Object>) properties, muleEvent.getMuleContext());

        responseMessage.setExceptionPayload(new DefaultExceptionPayload(new MessagingException(
            createStaticMessage(message), muleEvent, t)));

        return new DefaultMuleEvent(responseMessage, muleEvent);
    }
}
