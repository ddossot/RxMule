
package org.mule.rx.support;

import static org.mule.rx.support.WorkManagerCache.guessConnectorName;

import org.apache.commons.lang.Validate;
import org.mule.api.MuleException;
import org.mule.api.callback.SourceCallback;
import org.mule.api.source.MessageSource;
import org.mule.security.oauth.processor.AbstractListeningMessageProcessor;

import rx.functions.Action1;

public class SourceCallbackMessageSourceAdapter extends AbstractListeningMessageProcessor
    implements MessageSource
{
    private final Action1<SourceCallback> sourceCallbackConsumer;

    private volatile Thread subscriberThread;

    public SourceCallbackMessageSourceAdapter(final Action1<SourceCallback> sourceCallbackConsumer,
                                              final Object connector)
    {
        super(guessConnectorName(connector));

        Validate.notNull(sourceCallbackConsumer, "sourceCallbackConsumer can't be null");

        this.sourceCallbackConsumer = sourceCallbackConsumer;
    }

    @Override
    public void start() throws MuleException
    {
        subscriberThread = new Thread(new Runnable()
        {
            @Override
            public void run()
            {
                // this is often a blocking operation, with no means of cancellation :(
                // hence this rogue thread that we kill like a scruffy dog on close.
                sourceCallbackConsumer.call(SourceCallbackMessageSourceAdapter.this);
            }
        }, "SourceCallbackSubscriber-" + getFlowConstruct().getName());

        subscriberThread.start();
    }

    @Override
    public void stop() throws MuleException
    {
        if (subscriberThread != null)
        {
            // tables are being flipped right now
            subscriberThread.interrupt();

            subscriberThread = null;
        }
    }
}
