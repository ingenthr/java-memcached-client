package net.spy.memcached.vbucket.config;

import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import java.util.ArrayList;
import java.util.List;
import java.io.FileInputStream;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.IOException;

import net.spy.memcached.HashAlgorithm;

/**
 * @author alexander.sokolovsky.a@gmail.com
 */
public class DefaultConfigFactory implements ConfigFactory {

    public Config createConfigFromFile(String filename) {
        if (filename == null || "".equals(filename)) {
            throw new ConfigParsingException("Filename is empty.");
        }
        StringBuilder sb = new StringBuilder();
        try {
            FileInputStream fis = new FileInputStream(filename);
            BufferedReader reader = new BufferedReader(new InputStreamReader(fis));
            String str;
            while ((str = reader.readLine()) != null) {
                sb.append(str);
            }
        } catch (IOException e) {
            throw new ConfigParsingException("Exception reading input file: " + e.getMessage());
        }
        return createConfigFromString(sb.toString());
    }

    public Config createConfigFromString(String data) {
        try {
            JSONObject jsonObject = new JSONObject(data);
            return parseJSON(jsonObject);
        } catch (JSONException e) {
            throw new ConfigParsingException("Exception parsing JSON data: " + e.getMessage());
        }
    }

    public Config createConfigFromJSON(JSONObject jsonObject) {
        try {
            return parseJSON(jsonObject);
        } catch (JSONException e) {
            throw new ConfigParsingException("Exception parsing JSON data: " + e.getMessage());
        }
    }

    private HashAlgorithm lookupHashAlgorithm(String algorithm) {
        HashAlgorithm ha = HashAlgorithm.NATIVE_HASH;
        if ("crc".equalsIgnoreCase(algorithm)) {
            ha = HashAlgorithm.CRC32_HASH;
        } else if ("fnv1_32".equalsIgnoreCase(algorithm)) {
            ha = HashAlgorithm.FNV1_32_HASH;
        } else if ("fnv1_64".equalsIgnoreCase(algorithm)) {
            ha = HashAlgorithm.FNV1_64_HASH;
        } else if ("fnv1a_32".equalsIgnoreCase(algorithm)) {
            ha = HashAlgorithm.FNV1A_32_HASH;
        } else if ("fnv1a_64".equalsIgnoreCase(algorithm)) {
            ha = HashAlgorithm.FNV1A_64_HASH;
        } else if ("md5".equalsIgnoreCase(algorithm)) {
            ha = HashAlgorithm.KETAMA_HASH;
        }
        return ha;
    }

    private Config parseJSON(JSONObject jsonObject) throws JSONException {
	// the incoming config could be cache or EP object types, JSON envelope picked apart
	if (!jsonObject.has("vBucketServerMap" )) {
	    return parseCacheJSON(jsonObject);
	}
	return parseEpJSON(jsonObject.getJSONObject("vBucketServerMap"));
    }

    private Config parseCacheJSON(JSONObject jsonObject) throws JSONException {

	JSONArray nodes = jsonObject.getJSONArray("nodes");
        if (nodes.length() <= 0) {
            throw new ConfigParsingException("Empty nodes list.");
        }
        int serversCount = nodes.length();

	CacheConfig config = new CacheConfig(serversCount);
        populateServers(config, nodes);

	return config;
    }

    /* ep is for ep-engine, a.k.a. membase */
    private Config parseEpJSON(JSONObject jsonObject) throws JSONException {

        HashAlgorithm hashAlgorithm = lookupHashAlgorithm(jsonObject.getString("hashAlgorithm"));
        int replicasCount = jsonObject.getInt("numReplicas");
        if (replicasCount > VBucket.MAX_REPLICAS) {
            throw new ConfigParsingException("Expected number <= " + VBucket.MAX_REPLICAS + " for replicas.");
        }
        JSONArray servers = jsonObject.getJSONArray("serverList");
        if (servers.length() <= 0) {
            throw new ConfigParsingException("Empty servers list.");
        }
        int serversCount = servers.length();
        JSONArray vbuckets = jsonObject.getJSONArray("vBucketMap");
        int vbucketsCount = vbuckets.length();
        if (vbucketsCount == 0 || (vbucketsCount & (vbucketsCount - 1)) != 0) {
            throw new ConfigParsingException("Number of buckets must be a power of two, > 0 and <= " + VBucket.MAX_BUCKETS);
        }

        DefaultConfig config = new DefaultConfig(hashAlgorithm, serversCount, replicasCount, vbucketsCount);
        populateServers(config, servers);
        populateVbuckets(config, vbuckets);

        return config;
    }

    private void populateServers(DefaultConfig config, JSONArray servers) throws JSONException {
        List<String> serverNames = new ArrayList<String>();
        for (int i = 0; i < servers.length(); i++) {
            String server = servers.getString(i);
            serverNames.add(server);
        }
        config.setServers(serverNames);
    }

    private void populateServers(CacheConfig config, JSONArray nodes) throws JSONException {
	List<String> serverNames = new ArrayList<String>();
	for (int i = 0; i < nodes.length(); i++) {
	    JSONObject node = nodes.getJSONObject(i);
	    String webHostPort = node.getString("hostname");
	    String[] splitHostPort = webHostPort.split(":");
	    JSONObject portsList = node.getJSONObject("ports");
	    int port = portsList.getInt("direct");
	    serverNames.add(splitHostPort[0] + ":" + port);
	}
	config.setServers(serverNames);
    }

    private void populateVbuckets(Config config, JSONArray jsonVbuckets) throws JSONException {
        List<VBucket> vBuckets = new ArrayList<VBucket>();
        for (int i = 0; i < jsonVbuckets.length(); i++) {
            JSONArray rows = jsonVbuckets.getJSONArray(i);
            VBucket vbucket = new VBucket();
            for (int j = 0; j < rows.length(); j++) {
                vbucket.getServers()[j] = rows.getInt(j);
            }
            vBuckets.add(vbucket);
        }
        config.setVbuckets(vBuckets);
    }

}
