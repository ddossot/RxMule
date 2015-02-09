
package org.mule.rx;

import org.mule.api.MuleEvent;
import org.mule.api.MuleException;

import rx.functions.Action1;

/**
 * An {@link Action1} that simplifies dealing with {@link MuleException} thrown inside the
 * <code>call</code> method.
 */
public abstract class RxMuleAction1 implements Action1<MuleEvent>
{
    @Override
    public final void call(final MuleEvent muleEvent)
    {
        try
        {
            doCall(muleEvent);
        }
        catch (final Throwable t)
        {
            throw new RxMuleEventException(muleEvent, t);
        }
    }

    protected abstract void doCall(final MuleEvent muleEvent) throws MuleException;
}
