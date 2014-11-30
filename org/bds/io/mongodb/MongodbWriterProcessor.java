/*
 *  This file is part of the Heritrix web crawler (crawler.archive.org).
 *  MongoDB storage has been added for big data crawler to store crawled Internet data. 
 *  
 *  @email herman_cn@163.com
 *  @date 2014-10-21
 */

package org.bds.io.mongodb;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.archive.io.ReplayInputStream;
import org.archive.io.warc.WARCWriterPoolSettings;
import org.archive.modules.CrawlURI;
import org.archive.modules.ProcessResult;
import org.archive.modules.writer.WriterPoolProcessor;
import org.archive.spring.ConfigPath;
import org.archive.uid.RecordIDGenerator;
import org.bds.io.mongodb.MongodbParameters;
import org.bds.io.mongodb.MongodbWriter;
import org.bds.io.mongodb.MongodbWriterPool;
import org.json.JSONException;
import org.json.JSONObject;


/**
 * 
 * @author bds
 *
 */
public class MongodbWriterProcessor extends WriterPoolProcessor implements WARCWriterPoolSettings {
    @SuppressWarnings("unused")
    private static final long serialVersionUID = 333333333L;
    private static final Logger logger = 
        Logger.getLogger(MongodbWriterProcessor.class.getName());

    private ConcurrentMap<String, ConcurrentMap<String, AtomicLong>> stats = new ConcurrentHashMap<String, ConcurrentMap<String, AtomicLong>>();

    private AtomicLong urlsWritten = new AtomicLong();
    
    /**
     * @see org.bds.io.mongodb.MongodbParameters
     */
    MongodbParameters mongodbParameters = null;
   
    public synchronized MongodbParameters getMongodbParameters() {
    	return mongodbParameters;
    }
    public void setMongodbParameters(MongodbParameters mongodbParameters) {
    	this.mongodbParameters = mongodbParameters;
    }

    public long getDefaultMaxFileSize() {
    	if (mongodbParameters != null) {
			return (mongodbParameters.getDefaultMaxContentSizeInBytes());			
		} 
		return MongodbParameters.DEFAULT_MAX_CONTENT_SIZE_IN_BYTES;
    }
    
	/**
	 * Gets the default store paths.
	 *
	 * @return the default store paths
	 */
	@Override
	protected List<ConfigPath> getDefaultStorePaths() {
		return null;
	}

	/* (non-Javadoc)
	 * @see org.archive.modules.writer.WriterPoolProcessor#getMetadata()
	 */
	@Override
	public List<String> getMetadata() {
		return null;
	}

	/* (non-Javadoc)
	 * @see org.archive.io.warc.WARCWriterPoolSettings#getRecordIDGenerator()
	 */
	@Override
	public RecordIDGenerator getRecordIDGenerator() {
		return null;
	}
    
    
    @Override
    protected void setupPool(final AtomicInteger serialNo) {
    	setPool(new MongodbWriterPool(serialNo, getMongodbParameters(), this, getPoolMaxActive(), getMaxWaitForIdleMs()));
    }

    /**
     * Writes a CrawlURI and its associated data to mongodb.
     * 
     * Currently this method understands the following uri types: dns, http, and
     * https.
     * 
     * @param curi CrawlURI to process.
     * 
     */
    @Override
    protected ProcessResult innerProcessResult(CrawlURI puri) {
        CrawlURI curi = (CrawlURI)puri;  
        long recordLength = getRecordedSize(curi);
        ReplayInputStream ris = null;
      
        try {
            if (shouldWrite(curi)) {
                ris = curi.getRecorder().getRecordedInput().getReplayInputStream();
            	return write(curi, recordLength, ris);
            } else {
                copyForwardWriteTagIfDupe(curi);
            }
        } catch (IOException e) {
            curi.getNonFatalFailures().add(e);
            logger.log(Level.SEVERE, "Failed write of Records: " +
                curi.toString(), e);
        } catch (InterruptedException e) {
		}
        return ProcessResult.PROCEED;
    }


	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.archive.modules.Processor#shouldProcess(org.archive.modules.ProcessorURI
	 * )
	 */
	@Override
	protected boolean shouldProcess(CrawlURI curi) {
		// The super method is still checked, but only continue with 
		// process checking if it returns true.  This way the super class
		// overrides our checking.
		if (!super.shouldProcess(curi)) {
			return false;
		}
		
        // If failure, or we haven't fetched the resource yet, return
        if (curi.getFetchStatus() <= 0) {
            return false;
        }
        
        // If no recorded content at all, don't write record.
        long recordLength = curi.getContentSize();
        if (recordLength <= 0) {
            // getContentSize() should be > 0 if any material (even just
            // HTTP headers with zero-length body is available.
            return false;
        }
		// If we make it here, then we passed all our checks and we can assume
		// we should process the record.
		return true;
	}
    
	/**
	 * Whether the given CrawlURI should be written to archive files. Annotates
	 * CrawlURI with a reason for any negative answer.
	 * 
	 * @param curi
	 *            CrawlURI
	 * 
	 * @return true if URI should be written; false otherwise
	 */
	@Override
	protected boolean shouldWrite(CrawlURI curi) {
		// The old method is still checked, but only continue with the next
		// checks if it returns true.
		if (!super.shouldWrite(curi)) {
			return false;
		}

		// If the content exceeds the maxContentSize, then dont write.
		if (curi.getContentSize() > getMaxFileSizeBytes()) {
			// content size is too large
			curi.getAnnotations().add(ANNOTATION_UNWRITTEN + ":size");
			logger.log(Level.WARNING, "Content size for " + curi.getUURI() + " is too large (" + curi.getContentSize() + ") - maximum content size is: " + getMaxFileSizeBytes());
			return false;
		}
		// all tests pass, return true to write the content locally.
		return true;
	}


    protected ProcessResult write(final CrawlURI curi, long recordLength, InputStream in)
    throws IOException, InterruptedException {
        MongodbWriter mongodbWriter = (MongodbWriter) getPool().borrowFile();
      
        long position = mongodbWriter.getPosition();
        try {                   
        	mongodbWriter.write(curi, getHostAddress(curi), curi.getRecorder().getRecordedOutput(),
            		curi.getRecorder().getRecordedInput());
		} finally {
			// log total bytes written
			setTotalBytesWritten(getTotalBytesWritten() + (mongodbWriter.getPosition() - position));
			// return the hbaseWriter client back to the pool.
			getPool().returnFile(mongodbWriter);
        }
        return checkBytesWritten();
    }
    

    protected void addStats(Map<String, Map<String, Long>> substats) {
        for (String key: substats.keySet()) {
            // intentionally redundant here -- if statement avoids creating
            // unused empty map every time; putIfAbsent() ensures thread safety
            if (stats.get(key) == null) {
                stats.putIfAbsent(key, new ConcurrentHashMap<String, AtomicLong>());
            }
            
            for (String subkey: substats.get(key).keySet()) {
                AtomicLong oldValue = stats.get(key).get(subkey);
                if (oldValue == null) {
                    oldValue = stats.get(key).putIfAbsent(subkey, new AtomicLong(substats.get(key).get(subkey)));
                }
                if (oldValue != null) {
                    oldValue.addAndGet(substats.get(key).get(subkey));
                }
            }
        }
    }
    
    
    @Override
    protected JSONObject toCheckpointJson() throws JSONException {
        JSONObject json = super.toCheckpointJson();
        json.put("urlsWritten", urlsWritten);
        json.put("stats", stats);
        return json;
    }
    
    @Override
    protected void fromCheckpointJson(JSONObject json) throws JSONException {
        super.fromCheckpointJson(json);

        // conditionals below are for backward compatibility with old checkpoints
        
        if (json.has("urlsWritten")) {
            urlsWritten.set(json.getLong("urlsWritten"));
        }
        
        if (json.has("stats")) {
            HashMap<String, Map<String, Long>> cpStats = new HashMap<String, Map<String, Long>>();
            JSONObject jsonStats = json.getJSONObject("stats");
            if (JSONObject.getNames(jsonStats) != null) {
                for (String key1: JSONObject.getNames(jsonStats)) {
                    JSONObject jsonSubstats = jsonStats.getJSONObject(key1);
                    if (!cpStats.containsKey(key1)) {
                        cpStats.put(key1, new HashMap<String, Long>());
                    }
                    Map<String, Long> substats = cpStats.get(key1);

                    for (String key2: JSONObject.getNames(jsonSubstats)) {
                        long value = jsonSubstats.getLong(key2);
                        substats.put(key2, value);
                    }
                }
                addStats(cpStats);
            }
        }
    }
    
}