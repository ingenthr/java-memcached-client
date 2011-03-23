package net.spy.memcached.test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.logging.ConsoleHandler;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;

import net.spy.memcached.BinaryConnectionFactory;
import net.spy.memcached.ConnectionFactoryBuilder;
import net.spy.memcached.FailureMode;
import net.spy.memcached.HashAlgorithm;
import net.spy.memcached.MemcachedClient;

public class MemcacheTest5 {

    static {
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

    private MemcacheTest5() {
    }

    private static Object getWithTimeout(MemcachedClient client, String key, int timeout) throws Exception {
	Object result = null;
	Future<Object> f = client.asyncGet(key);
	try {
	    result = f.get(timeout, TimeUnit.MILLISECONDS);
	} catch (Exception e) {
	    f.cancel(true);
	    throw e;
	}
	return result;
    }

    private static Stats testLoop(int iterations, MemcachedClient client) {
	Stats s = new Stats();
	s.iterations = iterations;
	long start = System.currentTimeMillis();
	ArrayList<String> keys = new ArrayList<String>();
	for (int i = 0; i < iterations; i++) {
	    String key = "test" + System.currentTimeMillis() + "-" + i;
	    String value = "val" + System.currentTimeMillis() + "-" + i;
	    keys.add(key);
	    client.set(key, 0, value);
	}
	s.setTime = System.currentTimeMillis() - start;
	start = System.currentTimeMillis();
	for (String key : keys) {
	    try {
		getWithTimeout(client, key, 500);
	    } catch (Exception e) {
		s.exceptions++;
	    }
	}
	s.getTime = System.currentTimeMillis() - start;
	return s;
    }

    public static MemcachedClient createVanillaClient() throws IOException {
	List<InetSocketAddress> addrs = new ArrayList<InetSocketAddress>();
	addrs.add(new InetSocketAddress("10.1.5.121", 11210));
	addrs.add(new InetSocketAddress("10.1.5.122", 11210));
	ConnectionFactoryBuilder cfb = new ConnectionFactoryBuilder();
	cfb.setLocatorType(ConnectionFactoryBuilder.Locator.CONSISTENT)
	   .setHashAlg(HashAlgorithm.KETAMA_HASH)
	   .setFailureMode(FailureMode.Redistribute)
	   .setProtocol(ConnectionFactoryBuilder.Protocol.BINARY);
	return new MemcachedClient(cfb.build(), addrs);
    }

    public static MemcachedClient createEnhancedClient() throws Exception {
	List<URI> baseUris = new ArrayList<URI>();
	baseUris.add(new URI("http://10.1.5.121:8091/pools/"));
	baseUris.add(new URI("http://10.1.5.122:8091/pools/"));
	return new MemcachedClient(baseUris, "default", "default", "");
    }

    static void print(Stats s, Stats c) {
	float getAvgCurrent = (float) s.getTime / s.iterations;
	float setAvgCurrent = (float) s.getTime / s.iterations;
	float exceptionAvgCurrent = (float) s.exceptions / s.iterations;

	float getAvgCumulative = (float) c.getTime / c.iterations;
	float setAvgCumulative = (float) c.getTime / c.iterations;
	float exceptionAvgCumulative = (float) c.exceptions / c.iterations;

	System.out.printf("Current: %d iterations; avgGet=%f; avgSet=%f; exceptions=%d; avgException=%f\n", s.iterations, getAvgCurrent, setAvgCurrent, s.exceptions, exceptionAvgCurrent);
	System.out.printf("Cumulative: %d iterations; avgGet=%f; avgSet=%f; exceptions=%d; avgException=%f\n", c.iterations, getAvgCumulative, setAvgCumulative, c.exceptions, exceptionAvgCumulative);
	System.out.println();
    }

    public static void main(String[] args) throws Exception {
	MemcachedClient client = null;
	if (args.length > 0 && "-e".equals(args[0])) {
	    System.out.println("Using \"enhanced\" (vbucket aware) mode");
	    client = MemcacheTest5.createEnhancedClient();
	} else {
	    System.out.println("Using \"simple\" (non-vbucket aware) mode");
	    client = MemcacheTest5.createVanillaClient();
	}
	Stats cumulative = new Stats();
	while (true) {
	    try {
		Stats s = MemcacheTest5.testLoop(100, client);
		cumulative.add(s);
		print(s, cumulative);
		Thread.sleep(1000);
	    } catch (InterruptedException e) {
		return;
	    }
	}
    }

    static class Stats {

	public long iterations = 0;
	public long getTime = 0;
	public long setTime = 0;
	public long exceptions = 0;

	public void add(Stats s) {
	    this.iterations += s.iterations;
	    this.getTime += s.getTime;
	    this.setTime += s.setTime;
	    this.exceptions += s.exceptions;
	}
    }
}
