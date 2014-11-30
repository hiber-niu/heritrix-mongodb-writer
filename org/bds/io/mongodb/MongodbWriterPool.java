/*
 *  This file is part of the Heritrix web crawler (crawler.archive.org).
 *  MongoDB storage has been added for big data crawler to store crawled Internet data. 
 *  
 *  @email herman_cn@163.com
 *  @date 2014-10-21
 */

package org.bds.io.mongodb;

import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;
import org.archive.io.WriterPool;
import org.archive.io.WriterPoolMember;
import org.archive.io.WriterPoolSettings;


public class MongodbWriterPool extends WriterPool {
	
	private static final Logger logger = Logger.getLogger(MongodbWriterPool.class.getName());
	private MongodbParameters _parameters;
	
	/**
	 * Create a pool of MongodbWriter objects.
	 *
	 * @param cassandraSeeds
	 * @param cassandraPort
	 * @param parameters the {@link org.archive.io.mongodb.MongodbParameters} object containing your settings
	 * @param poolMaximumActive the maximum number of writers in the writer pool.
	 * @param poolMaximumWait the maximum waittime for all writers in the pool.
	 */
    public MongodbWriterPool(final AtomicInteger serial, final MongodbParameters parameters, final WriterPoolSettings settings,
            final int poolMaximumActive, final int poolMaximumWait) {
    	super(serial, settings, poolMaximumActive, poolMaximumWait);
    	_parameters = parameters;
    }
     
    /* (non-Javadoc)
     * @see org.archive.io.WriterPool#makeWriter()
     */
    @Override
    protected WriterPoolMember makeWriter() {
        return new MongodbWriter(serialNo, settings, _parameters);
    }
}