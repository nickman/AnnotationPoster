# AnnotationPoster
An HttpRPC Plugin for OpenTSDB to post Annotations by metric name and tags.

### Quickie Setup

Add these to your opentsdb.conf:

> tsd.http.rpc.plugins=net.opentsdb.tsd.AnnotationPoster

> tsd.core.plugin_path=&lt;plugin path&gt;

Copy the annotation poster jar into &lt;plugin path&gt;.
Start OpenTSDB.

Post or put annotations to **http://&lt;host&gt;:&lt;port&gt;/plugin/postannotation**
Body can be a single or array of JSON annotations.
