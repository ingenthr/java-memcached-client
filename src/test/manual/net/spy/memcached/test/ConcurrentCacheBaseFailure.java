/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package net.spy.memcached.test;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.logging.ConsoleHandler;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;

import net.spy.memcached.MemcachedClient;

public class ConcurrentCacheBaseFailure
{
    static
    {
	System.setProperty("net.spy.log.LoggerImpl", "net.spy.memcached.compat.log.SunLogger");
	Logger.getLogger("net.spy.memcached").setLevel(Level.FINE);

        //get the top Logger:
        Logger topLogger = java.util.logging.Logger.getLogger("");

        // Handler for console (reuse it if it already exists)
        Handler consoleHandler = null;
        //see if there is already a console handler
        for (Handler handler : topLogger.getHandlers()) {
            if (handler instanceof ConsoleHandler) {
                //found the console handler
                consoleHandler = handler;
                break;
            }
        }


        if (consoleHandler == null) {
            //there was no console handler found, create a new one
            consoleHandler = new ConsoleHandler();
            topLogger.addHandler(consoleHandler);
        }
        //set the console handler to fine:
        consoleHandler.setLevel(java.util.logging.Level.FINE);
    }

    public static void main(String[] args) throws Exception
    {
        final MemcacheService memcacheService = new MemcacheService();

        TimerTask task = new TimerTask()
        {
            int i = 0;
            public void run()
            {
                try
                {
                    String key = "mcTest" + System.currentTimeMillis();
                    String value = String.valueOf(System.currentTimeMillis());
                    long t1 = System.currentTimeMillis();
                    memcacheService.setInKeysBucket("k" + key, value);
                    long t2 = System.currentTimeMillis();
                    memcacheService.setInDefaultBucket("d" + key, value);
                    long t3 = System.currentTimeMillis();
                    Object result = memcacheService.getFromKeysBucket("k" + key);
                    assert(value.equals(result));
                    long t4 = System.currentTimeMillis();
                    result = memcacheService.getFromDefaultBucket("d" + key);
                    assert(value.equals(result));
                    long t5 = System.currentTimeMillis();

                    long setKeys, setDefault, getKeys, getDefault, total;
                    setKeys = t2-t1;
                    setDefault = t3-t2;
                    getKeys = t4-t3;
                    getDefault = t5-t4;
                    total = t5-t1;
                    System.out.printf("%d: tot=%d: setKey=%d; setDefault=%d; getKey=%d; getDefault=%d\n", i, total, setKeys, setDefault, getKeys, getDefault);
                }
                catch (Exception e)
                {
                    System.out.println(i + ": exception!");
		    System.err.println(e.getLocalizedMessage());
                }
                finally
                {
                    i++;
                }
            }
        };

        Timer timer = new Timer("MemcacheTest");
        timer.schedule(task, 0, 5000);
    }
}

class MemcacheService
{
    private MemcachedClient keyClient;
    private MemcachedClient defaultClient;

    public MemcacheService() throws Exception
    {
        String baseUri = "http://10.1.5.121:8091/pools";
        URI base = new URI(baseUri);
        List<URI> baseUris = new ArrayList<URI>();
        baseUris.add(base);
        keyClient = new MemcachedClient(baseUris, "keys", "keys", "");
        defaultClient = new MemcachedClient(baseUris, "default", "default", "");
    }

    private static Object getWithTimeout(MemcachedClient client, String key)
    {
        Object result = null;
        Future<Object> f = client.asyncGet(key);
        try
        {
            result = f.get(500, TimeUnit.MILLISECONDS);
        }
        catch (Exception e)
        {
            System.err.println(e.getLocalizedMessage());
            f.cancel(true);
        }
        return result;
    }

    public void setInKeysBucket(String name, String value)
    {
        keyClient.set(name, 0, value);
    }

    public void setInDefaultBucket(String name, String value)
    {
        defaultClient.set(name, 0, value);
    }

    public Object getFromKeysBucket(String name)
    {
        return getWithTimeout(keyClient, name);
    }

    public Object getFromDefaultBucket(String name)
    {
        return getWithTimeout(defaultClient, name);
    }
}
