
package org.mule.rx;

import org.apache.commons.lang.Validate;
import org.mule.api.MuleException;
import org.mule.api.source.MessageSource;
import org.mule.construct.Flow;

import rx.functions.Func1;

public abstract class RxMessageSourceProvider implements Func1<Flow, MessageSource>
{
    @Override
    public final MessageSource call(final Flow flow)
    {
        Validate.notNull(flow, "flow can't be null");

        try
        {
            return doCall(flow);
        }
        catch (final MuleException me)
        {
            throw new RuntimeException("Failed to create message source for flow: " + flow, me);
        }
    }

    public abstract MessageSource doCall(Flow flow) throws MuleException;
}
