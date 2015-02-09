
package org.mule.rx.support;

import javax.resource.spi.work.Work;

import org.mule.RequestContext;
import org.mule.api.MessagingException;
import org.mule.api.MuleEvent;
import org.mule.api.context.WorkManager;
import org.mule.rx.RxMuleEventException;

import rx.Subscriber;

@SuppressWarnings("deprecation")
public abstract class Subscribers
{
    public static void routeEventToSubscriber(final MuleEvent muleEvent,
                                              final Subscriber<? super MuleEvent> subscriber,
                                              final WorkManager workManager)
    {
        try
        {
            workManager.scheduleWork(new Work()
            {
                @Override
                public void run()
                {
                    subscriber.onNext(RequestContext.setEvent(muleEvent));
                }

                @Override
                public void release()
                {
                    // NOOP
                }
            });
        }
        catch (final Exception e)
        {
            routeErrorToSubscriber(e, muleEvent, subscriber);
        }
    }

    public static void routeErrorToSubscriber(final Throwable t,
                                              final MuleEvent muleEvent,
                                              final Subscriber<? super MuleEvent> subscriber)
    {
        if (t instanceof RxMuleEventException)
        {
            subscriber.onError(t);
        }
        else if (t instanceof MessagingException)
        {
            subscriber.onError(new RxMuleEventException((MessagingException) t));
        }
        else
        {
            subscriber.onError(new MessagingException(muleEvent, t));
        }
    }

    private Subscribers()
    {
        throw new UnsupportedOperationException();
    }
}
