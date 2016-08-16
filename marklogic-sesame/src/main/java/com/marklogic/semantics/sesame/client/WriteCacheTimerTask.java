/*
 * Copyright 2015-2016 MarkLogic Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/**
 * A timer that flushes a cache of triple add statements
 * periodically. The cache is represented as a Model.
 */
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
 * Timer implements write cache for efficient adding of triples
 *
 * @author James Fuller
 */
public class WriteCacheTimerTask extends TimerTask {

    private static Logger log = LoggerFactory.getLogger(WriteCacheTimerTask.class);

    private Model cache;
    private MarkLogicClient client;

    public static long DEFAULT_CACHE_SIZE = 500;

    public static long DEFAULT_CACHE_MILLIS = 500;
    public static long DEFAULT_INITIAL_DELAY = 300;

    private RDFFormat format = RDFFormat.NQUADS;

    private long cacheSize;
    private long cacheMillis;

    private Date lastCacheAccess = new Date();

    /**
     * constructor
     *
     * @param client
     */
    public WriteCacheTimerTask(MarkLogicClient client) {
        super();
        this.client = client;
        this.cache = new LinkedHashModel();
        this.cacheSize = DEFAULT_CACHE_SIZE;
        this.cacheMillis = DEFAULT_CACHE_MILLIS;
    }

    public WriteCacheTimerTask(MarkLogicClient client, long cacheSize) {
        super();
        this.client = client;
        this.cache = new LinkedHashModel();
        setCacheSize(cacheSize);
    }

    /**
     * return cacheSize
     *
     * @return
     */
    public long getCacheSize() {
        return this.cacheSize;
    }

    /**
     *  set cacheSize
     *
     * @param cacheSize
     */
    public void setCacheSize(long cacheSize) {
        this.cacheSize = cacheSize;
    }

    /**
     * getter cacheMillis
     *
     * @return
     */
    public long getCacheMillis() {
        return cacheMillis;
    }

    /**
     * setter cacheMillis
     *
     * @param cacheMillis
     */
    public void setCacheMillis(long cacheMillis) {
        this.cacheMillis = cacheMillis;
    }

    /**
     * tests to see if we should flush cache
     *
     */
    @Override
    public void run(){
        Date now = new Date();
        if ( this.cache.size() > this.cacheSize - 1 || (this.cache.size() > 0 && now.getTime() - this.lastCacheAccess.getTime() > this.cacheMillis)) {
            try {
                flush();
            } catch ( MarkLogicSesameException| InterruptedException e) {
                // cannot throw exception in runnable run(), best we log it
                log.warn("Exception thrown in other thread, when running writeCacheTimerTask.");
                log.warn(e.toString(),e );
            }
        } else {
            return;
        }
    }

    /**
     * flushes the cache, writing triples as graph
     *
     * @throws MarkLogicSesameException
     */
    private void flush() throws MarkLogicSesameException, InterruptedException {
        log.debug("flushing write cache:" + this.cache.size());
            try {
                ByteArrayOutputStream out = new ByteArrayOutputStream();
                Rio.write(this.cache, out, this.format);
                InputStream in = new ByteArrayInputStream(out.toByteArray());
                this.client.sendAdd(in, null, this.format);
                this.lastCacheAccess = new Date();
                this.cache.clear();
            } catch (RDFHandlerException | RDFParseException e) {
                throw new MarkLogicSesameException(e);
            }
    }

    /**
     * forces the cache to flush if there is anything in it
     *
     * @throws MarkLogicSesameException
     */
    public void forceRun() throws MarkLogicSesameException {
        try {
            if( this.cache.size() > 0) {
                flush();
            }
        } catch (InterruptedException e) {
            throw new MarkLogicSesameException(e);
        }
    }

    /**
     * add triple to cache Model
     *
     * @param subject
     * @param predicate
     * @param object
     * @param contexts
     */
    public synchronized void add(Resource subject, URI predicate, Value object, Resource... contexts) throws MarkLogicSesameException {
        if(this.cache.size() > this.cacheSize - 1){
            forceRun();
        }
        this.cache.add(subject,predicate,object,contexts);
    }

}