// This file is part of OpenTSDB.
// Copyright (C) 2010-2016  The OpenTSDB Authors.
//
// This program is free software: you can redistribute it and/or modify it
// under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 2.1 of the License, or (at your
// option) any later version.  This program is distributed in the hope that it
// will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
// of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
// General Public License for more details.  You should have received a copy
// of the GNU Lesser General Public License along with this program.  If not,
// see <http://www.gnu.org/licenses/>.
package net.opentsdb.tsd;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBufferFactory;
import org.jboss.netty.buffer.ChannelBufferInputStream;
import org.jboss.netty.buffer.ChannelBufferOutputStream;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.buffer.DirectChannelBufferFactory;
import org.jboss.netty.buffer.ReadOnlyChannelBuffer;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;

import net.opentsdb.core.TSDB;
import net.opentsdb.core.Tags;
import net.opentsdb.meta.Annotation;
import net.opentsdb.meta.TSUIDQuery;
import net.opentsdb.stats.Histogram;
import net.opentsdb.stats.StatsCollector;
import net.opentsdb.uid.UniqueId;
import net.opentsdb.utils.Config;
import net.opentsdb.utils.JSON;

/**
 * <p>Title: AnnotationPoster</p>
 * <p>Description: An HttpRPC Plugin for OpenTSDB to post Annotations by Metric name</p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>net.opentsdb.tsd.AnnotationPoster</code></p>
 */

public class AnnotationPoster extends HttpRpcPlugin {
	/** Instance logger */
	protected final Logger LOG = LoggerFactory.getLogger(getClass());
	/** The parent TSDB instance */
	protected TSDB tsdb;
	/** The config instance */
	protected Config config;
	/** ChannelBuffer allocator */
	protected final ChannelBufferFactory bufferFactory = new DirectChannelBufferFactory();
	/** Unique IDs for the metric names. */
	protected UniqueId metrics;
	/** Unique IDs for the tag names. */
	protected UniqueId tag_names;
	/** Unique IDs for the tag values. */
	protected UniqueId tag_values;
	/** Default typing object mapper */
	protected final ObjectMapper dtMapper = new ObjectMapper().enableDefaultTyping(ObjectMapper.DefaultTyping.NON_FINAL);
	/** Request monitoring */
	protected final Histogram monitor = new Histogram(16000, (short) 2, 100);
	/** Completed request counter */
	protected final AtomicLong completeRequestCounter = new AtomicLong();
	/** Incoming request counter */
	protected final AtomicLong incomingRequestCounter = new AtomicLong();
	/** Tag map type reference */
	protected final TypeReference<Map<String, String>> MAP_TYPE_REF = new TypeReference<Map<String, String>>(){};
	/** Outbound Tag map type reference */
	protected final TypeReference<Map<String, Object>> MAP_TYPE_OBJ_REF = new TypeReference<Map<String, Object>>(){}; 
	
	/** Tag map object reader */
	protected final  ObjectReader tagMapReader = JSON.getMapper().reader(MAP_TYPE_REF);
	/** Annotation object reader */
	protected final  ObjectReader annotationReader = JSON.getMapper().reader(Annotation.class);
	
	
	
	
	/** The charset for JSON */
	protected static final Charset UTF8 = Charset.forName("UTF8");
	/** Const for an empty json array */
	protected static final ChannelBuffer EMPTY_JSON_ARR = new ReadOnlyChannelBuffer(ChannelBuffers.wrappedBuffer("[]".getBytes(UTF8)));
	/** Const for an empty json object */
	protected static final ChannelBuffer EMPTY_JSON_OBJ = new ReadOnlyChannelBuffer(ChannelBuffers.wrappedBuffer("{}".getBytes(UTF8)));
	/** Charset used to convert Strings to byte arrays and back. */
	protected static final Charset CHARSET = Charset.forName("ISO-8859-1");
	/** The stats metric name prefix */
	public static final String METRIC_NAME = "http.plugin.apost.";
	
	
	
	/**
	 * We need the TSDB UniqueId instances to share cache, preload and configuration options.
	 * For now, this is the only way to get this. Although it's reflection, we only do it once
	 * at init time, so it's not a significant performamce hit.
	 * @param tsdb The parent TSDB instance
	 * @param fieldName The field name of the target UniqueId
	 * @return The named UniqueId
	 */
	protected static UniqueId reflect(final TSDB tsdb, final String fieldName) {
		try {
			final Field f = TSDB.class.getDeclaredField(fieldName);
			f.setAccessible(true);
			return (UniqueId) f.get(tsdb);
		} catch (Exception ex) {
			throw new RuntimeException(ex);
		}
	}

	/**
	 * {@inheritDoc}
	 * @see net.opentsdb.tsd.HttpRpcPlugin#initialize(net.opentsdb.core.TSDB)
	 */
	@Override
	public void initialize(final TSDB tsdb) {
		this.tsdb = tsdb;
		config = tsdb.getConfig();
		metrics = reflect(tsdb, "metrics");
		tag_names = reflect(tsdb, "tag_names");
		tag_values = reflect(tsdb, "tag_values");
		LOG.info("AnnotationPoster plugin initialized");		
	}

	/**
	 * {@inheritDoc}
	 * @see net.opentsdb.tsd.HttpRpcPlugin#shutdown()
	 */
	@Override
	public Deferred<Object> shutdown() {
		return Deferred.fromResult(null);
	}

	/**
	 * {@inheritDoc}
	 * @see net.opentsdb.tsd.HttpRpcPlugin#version()
	 */
	@Override
	public String version() {
		return "2.2.0.1";
	}
	
	/**
	 * {@inheritDoc}
	 * @see net.opentsdb.tsd.HttpRpcPlugin#collectStats(net.opentsdb.stats.StatsCollector)
	 */
	@Override
	public void collectStats(final StatsCollector collector) {
		collector.record(METRIC_NAME + "elapsed", monitor, null);
		collector.record(METRIC_NAME + "inrequests", incomingRequestCounter.get());
		collector.record(METRIC_NAME + "complete", completeRequestCounter.get());
	}

	/**
	 * {@inheritDoc}
	 * @see net.opentsdb.tsd.HttpRpcPlugin#getPath()
	 */
	@Override
	public String getPath() {
		return "/postannotation";
	}
	
	/**
	 * Parses the passed content to a raw json node
	 * @param buff The channel buffer containing the content
	 * @return the parsed node
	 * @throws Exception thrown on any parsing error
	 */
	protected JsonNode parseAnnotation(final ChannelBuffer buff) throws Exception {
		InputStream is = null;
		try {
			is = new ChannelBufferInputStream(buff);
			final ObjectMapper om = JSON.getMapper();
			return om.readTree(is);
		} finally {
			try { is.close(); } catch (Exception x) {/* No Op */}
		}
	}
	
	
	/**
	 * Marshals the passed object to JSON and writes it out to the passed output stream
	 * @param os The output stream to write to
	 * @param obj The object to marshal and write
	 * @return the number of bytes written
	 * @throws Exception thrown on any JSON marshalling or IO error
	 */
	protected int writeToOutput(final ChannelBufferOutputStream os, final Object obj) throws Exception {
		JSON.getMapper().writeValue(os, obj);
		return os.writtenBytes();
	}
	

	/**
	 * Returns a deferred TSUID string for the passed metric name and tags,
	 * creating the UIDs if any do not exist.
	 * @param metric The metric name
	 * @param tags The tags
	 * @return a deferred TSUID string
	 */
	protected Deferred<String> getOrCreate(final String metric, final Map<String, String> tags) {
		try {
			Tags.validateString("metric", metric);
			for(Map.Entry<String, String> tag : tags.entrySet()) {
				Tags.validateString("tagk", tag.getKey());
				Tags.validateString("tagv", tag.getValue());
			}
		} catch (Exception ex) {
			return Deferred.fromError(ex);
		}
		final Deferred<String> def = new Deferred<String>();
		final List<Deferred<byte[]>> createOrGetComplete = new ArrayList<Deferred<byte[]>>((tags.size() * 2) + 1);
		createOrGetComplete.add(metrics.getOrCreateIdAsync(metric));
		for(Map.Entry<String, String> tag : tags.entrySet()) {
			createOrGetComplete.add(tag_names.getOrCreateIdAsync(tag.getKey()));
			createOrGetComplete.add(tag_values.getOrCreateIdAsync(tag.getValue()));
		}
		Deferred.group(createOrGetComplete).addCallback(new Callback<Void, ArrayList<byte[]>>() {
			@Override
			public Void call(final ArrayList<byte[]> validated) throws Exception {
				TSUIDQuery.tsuidFromMetric(tsdb, metric, tags).addCallback(new Callback<Void, byte[]>() {
					@Override
					public Void call(final byte[] tsuidBytes) throws Exception {
						final String tsuidString = UniqueId.uidToString(tsuidBytes);
						LOG.info("Created TSUID [{}] for [{}]:[{}]", tsuidString, metric, tags);
						def.callback(tsuidString);
						return null;
					}
				});
				return null;
			}
		});				
		return def;
	}
	
	/**
	 * <p>Title: MetricId</p>
	 * <p>Description: A value reference class to index the metric name and tags</p> 
	 * <p><code>net.opentsdb.tsd.AnnotationPoster.MetricId</code></p>
	 */
	static class MetricId {
		final String metric;
		final Map<String, String> tags;
		public MetricId(String metric, Map<String, String> tags) {
			super();
			this.metric = metric;
			this.tags = tags;
		}
		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + ((metric == null) ? 0 : metric.hashCode());
			result = prime * result + ((tags == null) ? 0 : tags.hashCode());
			return result;
		}
		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			MetricId other = (MetricId) obj;
			if (metric == null) {
				if (other.metric != null)
					return false;
			} else if (!metric.equals(other.metric))
				return false;
			if (tags == null) {
				if (other.tags != null)
					return false;
			} else if (!tags.equals(other.tags))
				return false;
			return true;
		}
		
		
	}
	
	/**
	 * Validates that a tsuid and underlying UIDs exist for the passed annotation metric name and tags,
	 * then creates the corresponding annotations and saves them to hbase.
	 * @param startTime The start time of the http rpc call in ms.
	 * @param query The http plugin rpc call
	 * @param overwrite true if http request was a <b>PUT</b>, false if it was a <b>POST</b>
	 * @param nodes The parsed JSON nodes representing the annotations to write
	 */
	protected void lookupTsuidsAndSave(final long startTime, final HttpRpcPluginQuery query, final boolean overwrite, final JsonNode...nodes) {
		final boolean multi = nodes.length > 1;
		
		final String im = query.getQueryStringParam("includemetric");
		final boolean includeMetricTags = im!=null; 
		final Map<MetricId, Annotation> annotations = new ConcurrentHashMap<MetricId, Annotation>();
		final List<Deferred<Annotation>> completion = new ArrayList<Deferred<Annotation>>(nodes.length);
		for(final JsonNode node: nodes) {
			try {
				if(!node.has("metric")) throw new RuntimeException("No metric in request");
				if(!node.has("tags")) throw new RuntimeException("No tags in request");
				final String metric = node.get("metric").textValue();
				final Map<String, String> tags = tagMapReader.readValue(node.get("tags"));
				final MetricId metricId = new MetricId(metric, tags);
				
				getOrCreate(metric, tags).addCallback(new Callback<Deferred<Annotation>, String>() {
					@Override
					public Deferred<Annotation> call(final String tsuid) throws Exception {
						final Deferred<Annotation> annotationDef = new Deferred<Annotation>();
						completion.add(annotationDef);
						((ObjectNode)node).put("tsuid", tsuid);
						final Annotation annotation = annotationReader.readValue(node);
						annotation.syncToStorage(tsdb, overwrite).addCallback(new Callback<Void, Boolean>() {
							@Override
							public Void call(final Boolean success) throws Exception {
								if(success) {
									annotations.put(metricId, annotation);
								}
								annotationDef.callback(annotation);
								return null;
							}
						});
						return annotationDef;
					}
				});
			} catch (Exception ex) {
				LOG.error("Failed to process annotation", ex);
			}
		}
		Deferred.group(completion).addCallback(new Callback<Void, ArrayList<Annotation>>() {
			@Override
			public Void call(final ArrayList<Annotation> complete) throws Exception {
				if(annotations==null || annotations.isEmpty()) {							
					query.sendBuffer(HttpResponseStatus.OK, multi ? EMPTY_JSON_ARR : EMPTY_JSON_OBJ, "application/json");
					return null;
				}
				LOG.info("Received [{}] Annotations", annotations.size());
				final ChannelBuffer cb;
				if(multi) {
					cb = ChannelBuffers.dynamicBuffer(1024 * nodes.length, bufferFactory);
				} else {
					cb = ChannelBuffers.dynamicBuffer(1024, bufferFactory);
				}
				ChannelBufferOutputStream os = null;
				try {
					os = new ChannelBufferOutputStream(cb);							
					if(multi) {
						if(includeMetricTags) {
							List<JsonNode> onodes = new ArrayList<JsonNode>(annotations.size());
							for(Map.Entry<MetricId, Annotation> entry : annotations.entrySet()) {
								final MetricId mid = entry.getKey();
								final ObjectNode on = JSON.getMapper().valueToTree(entry.getValue());
								on.put("metric", mid.metric);
								on.putPOJO("tags", mid.tags);
								onodes.add(on);
							}
							writeToOutput(os, onodes);
						} else {
							writeToOutput(os, annotations.values());
						}
						
					} else {
						if(includeMetricTags) {
							final Map.Entry<MetricId, Annotation> ann = annotations.entrySet().iterator().next();
							final ObjectNode on = JSON.getMapper().valueToTree(ann.getValue());
							final MetricId mid = ann.getKey();
							on.put("metric", mid.metric);
							on.putPOJO("tags", mid.tags);								
							writeToOutput(os, on);
						} else {
							writeToOutput(os, annotations.values().iterator().next());
						}
					
					}					
					query.sendBuffer(HttpResponseStatus.OK, cb, "application/json");
					final long elapsed = System.currentTimeMillis() - startTime;
					if(elapsed < Integer.MAX_VALUE) {
						final int elapsedInt = (int)elapsed;
						monitor.add(elapsedInt);
					}
					completeRequestCounter.incrementAndGet();
				} catch (Exception ex) {
					LOG.error("Failed to send response", ex);
					try { query.internalError(ex); } catch (Exception x) {/* No Op */}
				} finally {
					try { os.flush(); } catch (Exception x) {/* No Op */}
					try { os.close(); } catch (Exception x) {/* No Op */}
				}
				return null;
			}
		});
	}
	
	/**
	 * Conerts the passed jackson array node to an array of the underlying json nodes
	 * @param arrayNode The array node to convert
	 * @return an array of json nodes
	 */
	protected JsonNode[] toArray(final ArrayNode arrayNode) {
		final int size = arrayNode.size();
		final JsonNode[] nodes = new JsonNode[size];
		for(int i = 0; i < size; i++) {
			nodes[i] = arrayNode.get(i);
		}
		return nodes;
	}

	/**
	 * {@inheritDoc}
	 * @see net.opentsdb.tsd.HttpRpcPlugin#execute(net.opentsdb.core.TSDB, net.opentsdb.tsd.HttpRpcPluginQuery)
	 */
	@Override
	public void execute(final TSDB tsdb, final HttpRpcPluginQuery query) throws IOException {
		final long startTime = System.currentTimeMillis();
		incomingRequestCounter.incrementAndGet();
	    final HttpMethod method = query.method();		    
	    if(!method.equals(HttpMethod.POST) && !method.equals(HttpMethod.PUT)) {
	    	final String errMsg = "AnnotationPoster does not support Http Method [" + method.getName() + "]";
	    	LOG.warn(errMsg);
	    	query.badRequest(new BadRequestException(HttpResponseStatus.METHOD_NOT_ALLOWED, errMsg));
	    	return;
	    }
	    final boolean overwrite = method.equals(HttpMethod.PUT);
	    if(!query.hasContent()) {
	    	final String errMsg = "No content passed to AnnotationPoster";
	    	LOG.warn(errMsg);
	    	query.badRequest(new BadRequestException(HttpResponseStatus.BAD_REQUEST, errMsg));
	    	return;	    	
	    }
	    final ChannelBuffer buff = query.request().getContent();
	    final JsonNode node;
	    try {
	    	node = parseAnnotation(buff);
	    } catch (Exception ex) {
	    	final String errMsg = "Failed to parse content";
	    	LOG.warn(errMsg);
	    	query.badRequest(new BadRequestException(HttpResponseStatus.BAD_REQUEST, errMsg, ex));
	    	return;	    		    	
	    }
	    final JsonNode[] allNodes = node.isArray() ? toArray((ArrayNode)node) : new JsonNode[]{node};
	    lookupTsuidsAndSave(startTime, query, overwrite, allNodes);


	}

}
