package net.spy.memcached;

import java.util.Iterator;
import java.util.SortedMap;
import java.util.TreeMap;

class KetamaIterator implements Iterator<MemcachedNode> {

    final String key;
    long hashVal;
    int remainingTries;
    int numTries = 0;
    final HashAlgorithm hashAlg;
    final TreeMap<Long, MemcachedNode> ketamaNodes;

    public KetamaIterator(final String k, final int t, TreeMap<Long, MemcachedNode> ketamaNodes, final HashAlgorithm hashAlg) {
	super();
	this.ketamaNodes = ketamaNodes;
	this.hashAlg = hashAlg;
	hashVal = hashAlg.hash(k);
	remainingTries = t;
	key = k;
    }

    private void nextHash() {
	// this.calculateHash(Integer.toString(tries)+key).hashCode();
	long tmpKey = hashAlg.hash((numTries++) + key);
	// This echos the implementation of Long.hashCode()
	hashVal += (int) (tmpKey ^ (tmpKey >>> 32));
	hashVal &= 0xffffffffL; /* truncate to 32-bits */
	remainingTries--;
    }

    public boolean hasNext() {
	return remainingTries > 0;
    }

    public MemcachedNode next() {
	try {
	    return getNodeForKey(hashVal);
	} finally {
	    nextHash();
	}
    }

    public void remove() {
	throw new UnsupportedOperationException("remove not supported");
    }

    MemcachedNode getNodeForKey(long hash) {
	final MemcachedNode rv;
	if (!ketamaNodes.containsKey(hash)) {
	    // Java 1.6 adds a ceilingKey method, but I'm still stuck in 1.5
	    // in a lot of places, so I'm doing this myself.
	    SortedMap<Long, MemcachedNode> tailMap = ketamaNodes.tailMap(hash);
	    if (tailMap.isEmpty()) {
		hash = ketamaNodes.firstKey();
	    } else {
		hash = tailMap.firstKey();
	    }
	}
	rv = ketamaNodes.get(hash);
	return rv;
    }
}
