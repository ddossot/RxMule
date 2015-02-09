
package org.mule.rx;

import org.apache.commons.lang.Validate;
import org.mule.api.MessagingException;
import org.mule.api.MuleEvent;

/**
 * A {@link RuntimeException} that encapsulates a MuleEvent.
 */
public class RxMuleEventException extends RuntimeException
{
    private static final long serialVersionUID = 1L;

    private final MuleEvent muleEvent;

    public RxMuleEventException(final MuleEvent muleEvent, final Throwable cause)
    {
        super(cause);
        this.muleEvent = notNull(muleEvent);
    }

    public RxMuleEventException(final String message, final MuleEvent muleEvent)
    {
        super(message);
        this.muleEvent = notNull(muleEvent);
    }

    public RxMuleEventException(final String message, final MuleEvent muleEvent, final Throwable cause)
    {
        super(message, cause);
        this.muleEvent = notNull(muleEvent);
    }

    public RxMuleEventException(final MessagingException me)
    {
        this(me.getEvent(), me);
    }

    public MuleEvent getMuleEvent()
    {
        return muleEvent;
    }

    private MuleEvent notNull(final MuleEvent muleEvent)
    {
        // this would be useless if Mule would use CLang3
        Validate.notNull(muleEvent, "muleEvent can't be null");
        return muleEvent;
    }
}
