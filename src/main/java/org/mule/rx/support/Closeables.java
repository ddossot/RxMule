
package org.mule.rx.support;

import java.io.Closeable;
import java.io.IOException;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Closeables implements Closeable
{
    private static final Logger LOGGER = LoggerFactory.getLogger(Closeables.class);

    private final Set<Closeable> closeables;

    public Closeables()
    {
        this.closeables = new CopyOnWriteArraySet<>();
    }

    public void register(final Closeable closeable)
    {
        closeables.add(closeable);
    }

    public void stopAndDeregister(final Closeable closeable) throws IOException
    {
        try
        {
            closeable.close();
        }
        finally
        {
            closeables.remove(closeable);
        }
    }

    @Override
    public void close() throws IOException
    {
        for (final Closeable closeable : closeables)
        {
            try
            {
                stopAndDeregister(closeable);
            }
            catch (final IOException ioe)
            {
                LOGGER.error("Failed to stop: {}", closeable, ioe);
            }
        }

        closeables.clear();
    }
}
