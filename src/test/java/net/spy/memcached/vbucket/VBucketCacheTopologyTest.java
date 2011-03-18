package net.spy.memcached.vbucket;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.naming.ConfigurationException;
import junit.framework.TestCase;
import net.spy.memcached.MemcachedClient;
import org.jboss.netty.logging.Slf4JLoggerFactory;
import org.membase.jmembase.JMembase;

/**
 *
 */
public class VBucketCacheTopologyTest extends TestCase {

    public void testBasicMock() throws IOException, ConfigurationException, URISyntaxException {

	JMembase membMock = new JMembase(8091, 300, 1024, JMembase.DefaultType.CACHE);
	Thread t = new Thread(membMock, "mock membase REST");
        t.setDaemon(true);
        t.start();
	try {
	    Thread.sleep(500);
	} catch (InterruptedException ex) {
	    Logger.getLogger(VBucketCacheTopologyTest.class.getName()).log(Level.SEVERE, null, ex);
	}

	ArrayList<URI> baselist = new ArrayList<URI>();
	assert baselist.add(new URI("http://localhost:8091/pools"));
	MemcachedClient mc = new MemcachedClient(baselist, "default", null, null);

	mc.set("hello", 0, "world");
	assertEquals("world", mc.get("hello"));


	return;
    }

        public void testNodeFailsCacheAndBase() throws IOException, ConfigurationException, URISyntaxException {

	JMembase membMock = new JMembase(8091, 300, 1024, JMembase.DefaultType.CACHE);
	Thread t = new Thread(membMock, "mock membase REST");
        t.setDaemon(true);
        t.start();
	try {
	    Thread.sleep(500);
	} catch (InterruptedException ex) {
	    Logger.getLogger(VBucketCacheTopologyTest.class.getName()).log(Level.SEVERE, null, ex);
	}

	ArrayList<URI> baselist = new ArrayList<URI>();
	assert baselist.add(new URI("http://localhost:8091/pools"));
	MemcachedClient mc = new MemcachedClient(baselist, "default", null, null);

	mc.set("hello", 0, "world");
	assertEquals("world", mc.get("hello"));


	return;
    }

}
