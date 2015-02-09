
package org.mule.rx;

import org.mule.api.MuleEvent;
import org.mule.api.MuleException;

import rx.functions.Func1;

/**
 * A {@link Func1} that simplifies dealing with {@link MuleException} thrown inside the
 * <code>call</code> method.
 */
public abstract class RxMuleFunc1<R> implements Func1<MuleEvent, R>
{
    @Override
    public final R call(final MuleEvent muleEvent)
    {
        try
        {
            return doCall(muleEvent);
        }
        catch (final Throwable t)
        {
            throw new RxMuleEventException(muleEvent, t);
        }
    }

    protected abstract R doCall(final MuleEvent muleEvent) throws MuleException;
}
