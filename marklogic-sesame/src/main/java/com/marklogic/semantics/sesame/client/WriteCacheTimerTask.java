package com.marklogic.semantics.sesame.client;

import com.marklogic.semantics.sesame.MarkLogicSesameException;
import org.openrdf.model.Model;
import org.openrdf.model.Resource;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.impl.LinkedHashModel;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFParseException;
import org.openrdf.rio.Rio;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.util.Date;
import java.util.TimerTask;

/**
 * A timer task that flushes a cache of pending triple add statements
 * periodically. The cache is represented as a Model.
 */
public class WriteCacheTimerTask extends TimerTask {

    private static Logger log = LoggerFactory.getLogger(WriteCacheTimerTask.class);

    private Model cache;
    private MarkLogicClient client;

    public static long DEFAULT_CACHE_MILLIS = 750;
    public static long DEFAULT_INITIAL_DELAY = 0;
    private static long DEFAULT_CACHE_SIZE = 750;

    private long cacheSize = DEFAULT_CACHE_SIZE;
    private long cacheMillis = DEFAULT_CACHE_MILLIS;
    private Date lastCacheAccess = new Date();

    public WriteCacheTimerTask(MarkLogicClient client) {
        super();
        this.client = client;
        this.cache = new LinkedHashModel();
    }

    public long getCacheSize() {
        return this.cacheSize;
    }

    public void setCacheSize(long cacheSize) {
        this.cacheSize = cacheSize;
    }

    public long getCacheMillis() {
        return cacheMillis;
    }

    public void setCacheMillis(long cacheMillis) {
        this.cacheMillis = cacheMillis;
    }

    @Override
    public void run() {
        Date now = new Date();
        if ( cache.size() > cacheSize || cache.size() > 0 && now.getTime() - lastCacheAccess.getTime() > cacheMillis) {
            log.debug("Cache stale, flushing");
            try {
                flush();
            } catch (MarkLogicSesameException e) {
                e.printStackTrace();
            }
        } else {
            return;
        }
    }
    
    private synchronized void flush() throws MarkLogicSesameException {
            log.info("flushing write cache");
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            RDFFormat format = RDFFormat.NQUADS;
            try {
                Rio.write(cache, out, format);
                InputStream in = new ByteArrayInputStream(out.toByteArray());
                Resource[] contexts = new Resource[cache.contexts().size()];
                int i =0;
                for(Resource ctx:cache.contexts()){
                    contexts[i]= ctx;
                    i++;
                }
                client.sendAdd(in, null, format, contexts);
            } catch (RDFHandlerException e) {
                e.printStackTrace();
            } catch (RDFParseException e) {
                e.printStackTrace();
            }
            lastCacheAccess = new Date();
            cache.clear();
    }
    
    public void forceRun() throws MarkLogicSesameException {
        if(cache.size()>0) flush();
    }

    public synchronized void add(Resource subject, URI predicate, Value object, Resource... contexts) {
        cache.add(subject,predicate,object,contexts);
    }
}