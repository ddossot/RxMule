
package org.mule.rx.support;

import static org.mule.rx.support.Subscribers.routeErrorToSubscriber;

import org.apache.commons.lang.Validate;
import org.mule.api.MuleContext;
import org.mule.api.MuleEvent;
import org.mule.api.exception.MessagingExceptionHandler;
import org.mule.api.processor.ProcessingStrategy;
import org.mule.construct.Flow;
import org.mule.exception.DefaultMessagingExceptionStrategy;

import rx.Subscriber;

public class SubscriberFlow extends Flow
{
    private final Subscriber<? super MuleEvent> subscriber;

    public SubscriberFlow(final Subscriber<? super MuleEvent> subscriber,
                          final String name,
                          final MuleContext muleContext)
    {
        super(name, muleContext);

        Validate.notNull(subscriber, "subscriber can't be null");
        Validate.notEmpty(name, "name can't be empty");
        Validate.notNull(muleContext, "muleContext can't be null");

        this.subscriber = subscriber;
    }

    @Override
    public void setProcessingStrategy(final ProcessingStrategy processingStrategy)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public MessagingExceptionHandler getExceptionListener()
    {
        return new DefaultMessagingExceptionStrategy(muleContext)
        {
            @Override
            protected void doHandleException(final Exception e, final MuleEvent muleEvent)
            {
                routeErrorToSubscriber(e, muleEvent, subscriber);

                super.doHandleException(e, muleEvent);
            }
        };
    }
}
