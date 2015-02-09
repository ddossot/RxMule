
package org.mule.rx.support;

import static org.apache.commons.collections.CollectionUtils.isNotEmpty;

import java.io.Closeable;
import java.io.IOException;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

import org.apache.commons.lang.Validate;
import org.mule.MessageExchangePattern;
import org.mule.api.MuleContext;
import org.mule.api.MuleException;
import org.mule.api.endpoint.EndpointCache;
import org.mule.api.endpoint.ImmutableEndpoint;
import org.mule.api.endpoint.InboundEndpoint;
import org.mule.api.endpoint.OutboundEndpoint;
import org.mule.api.lifecycle.Stoppable;
import org.mule.endpoint.SimpleEndpointCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CloseableEndpointCache implements EndpointCache, Closeable
{
    private static final Logger LOGGER = LoggerFactory.getLogger(CloseableEndpointCache.class);

    private final MuleContext muleContext;
    private final SimpleEndpointCache delegate;
    private final Set<ImmutableEndpoint> endpoints;

    public CloseableEndpointCache(final MuleContext muleContext)
    {
        Validate.notNull(muleContext, "muleContext can't be null");

        this.muleContext = muleContext;
        this.delegate = new SimpleEndpointCache(muleContext);
        this.endpoints = new CopyOnWriteArraySet<>();

        LOGGER.debug("Initialized.");
    }

    @Override
    public InboundEndpoint getInboundEndpoint(final String uri, final MessageExchangePattern mep)
        throws MuleException
    {
        return register(delegate.getInboundEndpoint(uri, mep));
    }

    @Override
    public OutboundEndpoint getOutboundEndpoint(final String uri,
                                                final MessageExchangePattern mep,
                                                final Long responseTimeoutMillis) throws MuleException
    {
        return register(delegate.getOutboundEndpoint(uri, mep, responseTimeoutMillis));
    }

    @Override
    public void close() throws IOException
    {
        LOGGER.info("Closing with {} registered endpoints.", endpoints.size());

        for (final ImmutableEndpoint endpoint : endpoints)
        {
            remove(endpoint);
        }
    }

    private void remove(final ImmutableEndpoint endpoint)
    {
        if (endpoint instanceof Stoppable && isNotEmpty(endpoint.getMessageProcessors()))
        {
            try
            {
                ((Stoppable) endpoint).stop();
            }
            catch (final MuleException me)
            {
                LOGGER.error("Failed to stop endpoint: {}", endpoint, me);
            }
        }

        try
        {
            muleContext.getRegistry().unregisterEndpoint(endpoint.getName());
        }
        catch (final MuleException me)
        {
            LOGGER.error("Failed to unregister endpoint: {}", endpoint, me);
        }

        endpoints.remove(endpoint);
    }

    private <T extends ImmutableEndpoint> T register(final T endpoint) throws MuleException
    {
        muleContext.getRegistry().registerEndpoint(endpoint);
        endpoints.add(endpoint);

        LOGGER.debug("Registered: {}", endpoint);

        return endpoint;
    }
}
