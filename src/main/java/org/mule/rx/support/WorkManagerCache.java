
package org.mule.rx.support;

import static org.springframework.util.ReflectionUtils.findMethod;
import static org.springframework.util.ReflectionUtils.invokeMethod;
import static org.springframework.util.ReflectionUtils.makeAccessible;

import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.commons.lang.Validate;
import org.mule.api.MetadataAware;
import org.mule.api.MuleContext;
import org.mule.api.MuleException;
import org.mule.api.NameableObject;
import org.mule.api.context.WorkManager;
import org.mule.transport.AbstractConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WorkManagerCache implements Closeable
{
    private static final Logger LOGGER = LoggerFactory.getLogger(WorkManagerCache.class);

    private final MuleContext muleContext;
    private final ConcurrentMap<String, WorkManager> workManagers;

    public WorkManagerCache(final MuleContext muleContext)
    {
        Validate.notNull(muleContext, "muleContext can't be null");

        this.muleContext = muleContext;
        this.workManagers = new ConcurrentHashMap<>();
    }

    public MuleContext getMuleContext()
    {
        return muleContext;
    }

    @Override
    public void close() throws IOException
    {
        LOGGER.info("Closing with {} work manager(s).", workManagers.size());

        for (final Map.Entry<String, WorkManager> wme : workManagers.entrySet())
        {
            try
            {
                wme.getValue().dispose();
            }
            catch (final Exception e)
            {
                LOGGER.error("Failed to dispose work manager: {} : {}", wme.getKey(), wme.getValue(), e);
            }
        }

        workManagers.clear();
    }

    public WorkManager getReceiverWorkManager(final Object connector)
    {
        if (connector instanceof AbstractConnector)
        {
            try
            {
                // a glorious hack around getReceiverWorkManager being protected
                final Method m = findMethod(AbstractConnector.class, "getReceiverWorkManager");
                if (m != null)
                {
                    makeAccessible(m);
                    return (WorkManager) m.invoke(connector);
                }
            }
            catch (final Throwable t)
            {
                LOGGER.warn("Unexpectedly failed to call getReceiverWorkManager on AbstractConnector."
                            + "Falling back to a custom work manager.", t);
            }
        }

        return getOrCreateReceiverWorkManager(guessConnectorName(connector));
    }

    static String guessConnectorName(final Object connector)
    {
        if (connector instanceof MetadataAware)
        {
            final MetadataAware mac = (MetadataAware) connector;

            return mac.getModuleName() + "-" + mac.getModuleVersion();
        }
        else if (connector instanceof NameableObject)
        {
            return connector.getClass().getName() + "-" + ((NameableObject) connector).getName();
        }

        // DevKit connectors implement a local *copy* of org.mule.api.MetadataAware
        final Method getModuleNameMethod = findMethod(connector.getClass(), "getModuleName");
        final Method getModuleVersionMethod = findMethod(connector.getClass(), "getModuleVersion");

        if (getModuleNameMethod != null && getModuleVersionMethod != null)
        {
            return invokeMethod(getModuleNameMethod, connector) + "-"
                   + invokeMethod(getModuleVersionMethod, connector);
        }
        else
        {
            return connector.getClass().getName();
        }

    }

    private WorkManager getOrCreateReceiverWorkManager(final String name)
    {
        Validate.notEmpty(name, "name can't be empty");

        final WorkManager newWorkManager = muleContext.getDefaultMessageReceiverThreadingProfile()
            .createWorkManager(name, muleContext.getConfiguration().getShutdownTimeout());

        try
        {
            newWorkManager.start();
        }
        catch (final MuleException me)
        {
            LOGGER.warn("Failed to create a work manager for: {}. Falling back to the global work manager.",
                name, me);

            return muleContext.getWorkManager();
        }

        final WorkManager existingWorkManager = workManagers.putIfAbsent(name, newWorkManager);

        if (existingWorkManager != null)
        {
            // oh noes, another thread created the same work manager, we need to discard this new
            // one!
            newWorkManager.dispose();

            return existingWorkManager;
        }

        return newWorkManager;
    }
}
