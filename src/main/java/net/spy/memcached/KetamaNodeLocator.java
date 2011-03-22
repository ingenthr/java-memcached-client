package net.spy.memcached;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import net.spy.memcached.compat.SpyObject;
import net.spy.memcached.util.DefaultKetamaNodeLocatorConfiguration;
import net.spy.memcached.util.KetamaNodeLocatorConfiguration;
import net.spy.memcached.vbucket.config.Config;

/**
 * This is an implementation of the Ketama consistent hash strategy from
 * last.fm.  This implementation may not be compatible with libketama as
 * hashing is considered separate from node location.
 *
 * Note that this implementation does not currently supported weighted nodes.
 *
 * @see <a href="http://www.last.fm/user/RJ/journal/2007/04/10/392555/">RJ's blog post</a>
 */
public final class KetamaNodeLocator extends SpyObject implements NodeLocator {


	private volatile TreeMap<Long, MemcachedNode> ketamaNodes;
	final Collection<MemcachedNode> allNodes;

	final HashAlgorithm hashAlg;
	final KetamaNodeLocatorConfiguration config;


	public KetamaNodeLocator(List<MemcachedNode> nodes, HashAlgorithm alg) {
        this(nodes, alg, new DefaultKetamaNodeLocatorConfiguration());
	}

    public KetamaNodeLocator(List<MemcachedNode> nodes, HashAlgorithm alg, KetamaNodeLocatorConfiguration conf) {
		super();
		allNodes = nodes;
		hashAlg = alg;
		config = conf;

		setKetamaNodes(nodes);

    }

	private KetamaNodeLocator(TreeMap<Long, MemcachedNode> smn,
			Collection<MemcachedNode> an, HashAlgorithm alg, KetamaNodeLocatorConfiguration conf) {
		super();
		ketamaNodes=smn;
		allNodes=an;
		hashAlg=alg;
        config=conf;
	}

	public Collection<MemcachedNode> getAll() {
		return allNodes;
	}

	public MemcachedNode getPrimary(final String k) {
		MemcachedNode rv=getNodeForKey(hashAlg.hash(k));
		assert rv != null : "Found no node for key " + k;
		return rv;
	}

	long getMaxKey() {
		return getKetamaNodes().lastKey();
	}

	MemcachedNode getNodeForKey(long hash) {
		final MemcachedNode rv;
		if(!ketamaNodes.containsKey(hash)) {
			// Java 1.6 adds a ceilingKey method, but I'm still stuck in 1.5
			// in a lot of places, so I'm doing this myself.
			SortedMap<Long, MemcachedNode> tailMap=getKetamaNodes().tailMap(hash);
			if(tailMap.isEmpty()) {
				hash=getKetamaNodes().firstKey();
			} else {
				hash=tailMap.firstKey();
			}
		}
		rv=getKetamaNodes().get(hash);
		return rv;
	}

	public Iterator<MemcachedNode> getSequence(String k) {
		return new KetamaIterator(k, allNodes.size(), getKetamaNodes(), hashAlg);
	}

	public NodeLocator getReadonlyCopy() {
		TreeMap<Long, MemcachedNode> smn=new TreeMap<Long, MemcachedNode>(
			getKetamaNodes());
		Collection<MemcachedNode> an=
			new ArrayList<MemcachedNode>(allNodes.size());

		// Rewrite the values a copy of the map.
		for(Map.Entry<Long, MemcachedNode> me : smn.entrySet()) {
			me.setValue(new MemcachedNodeROImpl(me.getValue()));
		}
		// Copy the allNodes collection.
		for(MemcachedNode n : allNodes) {
			an.add(new MemcachedNodeROImpl(n));
		}

		return new KetamaNodeLocator(smn, an, hashAlg, config);
	}

    public void updateLocator(List<MemcachedNode> nodes, Config config) {
	getLogger().info("Reconfiguring node locator based upon request.");
	setKetamaNodes(nodes);
    }

    /**
     * @return the ketamaNodes
     */
    protected TreeMap<Long, MemcachedNode> getKetamaNodes() {
	return ketamaNodes;
    }

    protected void setKetamaNodes(List<MemcachedNode> nodes) {
	TreeMap<Long, MemcachedNode> newNodeMap = new TreeMap<Long, MemcachedNode>();
	int numReps= config.getNodeRepetitions();
	for(MemcachedNode node : nodes) {
		// Ketama does some special work with md5 where it reuses chunks.
		if(hashAlg == HashAlgorithm.KETAMA_HASH) {
			for(int i=0; i<numReps / 4; i++) {
				byte[] digest=HashAlgorithm.computeMd5(config.getKeyForNode(node, i));
				for(int h=0;h<4;h++) {
					Long k = ((long)(digest[3+h*4]&0xFF) << 24)
						| ((long)(digest[2+h*4]&0xFF) << 16)
						| ((long)(digest[1+h*4]&0xFF) << 8)
						| (digest[h*4]&0xFF);
					newNodeMap.put(k, node);
					getLogger().debug("Adding node %s in position %d", node, k);
				}

			}
		} else {
			for(int i=0; i<numReps; i++) {
				newNodeMap.put(hashAlg.hash(config.getKeyForNode(node, i)), node);
			}
		}
	}
	assert newNodeMap.size() == numReps * nodes.size();
	ketamaNodes = newNodeMap;

    }

}
