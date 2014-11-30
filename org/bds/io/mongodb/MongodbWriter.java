/*
 *  This file is part of the Heritrix web crawler (crawler.archive.org).
 *  MongoDB storage has been added for big data crawler to store crawled Internet data. 
 *  
 *  @email herman_cn@163.com
 *  @date 2014-10-21
 */

package org.bds.io.mongodb;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.UnknownHostException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.archive.io.RecordingInputStream;
import org.archive.io.RecordingOutputStream;
import org.archive.io.ReplayInputStream;
import org.archive.io.WriterPoolMember;
import org.archive.io.WriterPoolSettings;
import org.archive.modules.CrawlURI;
import org.archive.util.ArchiveUtils;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;


/**
 * WARC implementation.
 *
 * <p>Assumption is that the caller is managing access to this
 * WARCWriter ensuring only one thread accessing this WARC instance
 * at any one time.
 * 
 * <p>While being written, WARCs have a '.open' suffix appended.
 *
 * @contributor stack
 * @version $Revision: 4604 $ $Date: 2006-09-05 22:38:18 -0700 (Tue, 05 Sep 2006) $
 */
public class MongodbWriter extends WriterPoolMember
implements Serializer {
    
	private static final Logger logger = Logger.getLogger(MongodbWriterPool.class.getName());

    private MongodbParameters _mongodbParameters;
    private MongoClient  _mongoclient;
    
    
    public MongodbWriter(final AtomicInteger serialNo, final WriterPoolSettings settings,
    		MongodbParameters parameters) {
        super(serialNo, settings, "");
        
        _mongodbParameters = parameters;
        try {
			this._mongoclient = new MongoClient(parameters.getHost(), parameters.getPort());
		} catch (UnknownHostException e) {
			 logger.log(Level.SEVERE, "Unknown mongodb host: " + parameters.getHost(), e);
		}
    }
    
    public MongodbParameters getMongodbParameters() {
    	return _mongodbParameters;
    }
    
    public DBCollection getDBCollection() {
    	DB db = _mongoclient.getDB(_mongodbParameters.getDatabase());
    	boolean auth = true;
    	if(_mongodbParameters.getUser().equals("") == false)
    		auth = db.authenticate(_mongodbParameters.getUser(), _mongodbParameters.getPassword().toCharArray());
    	if(auth) {
    		 return db.getCollection(_mongodbParameters.getCollection());
    	} else {
    		 logger.log(Level.SEVERE, "MongoDB authentication is failed! Check your mongodb parameters!");
    		 this._mongoclient.close();
    	}
		return null;
    }
    
    
    
	/**
	 * Write the crawled output to the configured MongoDB table.
	 * Write each row key as the url with reverse domain and optionally process any content.
	 *
	 * @param curi URI of crawled document
	 * @param ip IP of remote machine.
	 * @param recordingOutputStream recording input stream that captured the response
	 * @param recordingInputStream recording output stream that captured the GET request
	 *
	 * @throws IOException Signals that an I/O exception has occurred.
	 * @throws InterruptedException
	 */
	public void write(final CrawlURI curi, final String ip, final RecordingOutputStream recordingOutputStream,
			final RecordingInputStream recordingInputStream) throws IOException, InterruptedException {

	    // Generate the target url of the crawled document
		String url = curi.toString();

		if (getMongodbParameters().isRemoveMissingPages() &&
				(curi.getFetchStatus() == HttpURLConnection.HTTP_NOT_FOUND || curi.getFetchStatus() == HttpURLConnection.HTTP_GONE)) {		
 			//Just skip the missing page.
			return;
		} else {
			
			// The timestamp is the curi fetch time in microseconds
//			long timestamp = curi.getFetchBeginTime()*1000;

			// The batchSize is the number of docs that should be inserted in a batch.
//			int batchSize = getMongodbParameters().getBulkDocNumber();
			
			DBObject doc = new BasicDBObject();
//			doc.put("timestamp", timestamp);
			
			
			// write the target url to the url column
			doc.put(getMongodbParameters().getUrlColumnName(), url);

			// write the target ip to the ip column
			doc.put(getMongodbParameters().getIpColumnName(), ip);

			// is the url part of the seed url (the initial url(s) used to start the crawl)
			if (curi.isSeed()) {
				doc.put(getMongodbParameters().getIsSeedColumnName(), true);
			}

			if (curi.getPathFromSeed() != null && curi.getPathFromSeed().trim().length() > 0) {
				doc.put(getMongodbParameters().getPathFromSeedColumnName(), curi.getPathFromSeed().trim());
			}

			// write the Via string
			String viaStr = (curi.getVia() != null) ? curi.getVia().toString().trim() : null;
			if (viaStr != null && viaStr.length() > 0) {
				doc.put(getMongodbParameters().getViaColumnName(), viaStr);
			}

			String fetchTime = ArchiveUtils.get14DigitDate(curi.getFetchBeginTime());
			String localTimeZone = getMongodbParameters().getTimeZone();
			Date localDate = null; 
			SimpleDateFormat formatter;  
			SimpleDateFormat parser;
			String localFetchTime = "";
			
			if(localTimeZone != null) {		
				parser = new SimpleDateFormat("yyyyMMddHHmmss");
				parser.setTimeZone(TimeZone.getTimeZone("UTC"));
				try {
					localDate = parser.parse(fetchTime);
				} catch (ParseException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
				formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
				formatter.setTimeZone(TimeZone.getTimeZone(localTimeZone));
				localFetchTime = formatter.format(localDate);
			}
			
			if (localFetchTime != null && !localFetchTime.isEmpty()) {
				doc.put(getMongodbParameters().getProcessedAtColumnName(), localFetchTime);
			}

			// Write the Crawl Request to the Put object
			if (recordingOutputStream.getSize() > 0) {
			    String crawlRequest = getEncodedStringFromInputStream(recordingOutputStream.getReplayInputStream(),
                        (int) recordingInputStream.getSize(), curi);

				doc.put(getMongodbParameters().getRequestColumnName(), crawlRequest);
			}

			// Write the Crawl Response to the Put object
			ReplayInputStream replayInputStream = recordingInputStream.getReplayInputStream();
			try {
			    // Read in the response fully into a byte array
			    String crawlResponse = getEncodedStringFromInputStream(replayInputStream, (int) recordingInputStream.getSize(), curi);

                // reset the input steam for the content processor
                replayInputStream = recordingInputStream.getReplayInputStream();
                replayInputStream.setToResponseBodyStart();

                // If it's configured, try to separate the HTTP response headers and store them in another column
			    if (getMongodbParameters().isSeparateHeaders()) {
			        int contentIndex = getContentIndex(crawlResponse);
			        if (contentIndex != -1) {
			            String headers = crawlResponse.substring(0, contentIndex);
			            doc.put(getMongodbParameters().getHeadersColumnName(), headers);
			            crawlResponse = crawlResponse.substring(contentIndex);
			        }
			    }  
                             
			    int maxSize = getMongodbParameters().getDefaultMaxContentSizeInBytes();
			    if (maxSize > 0 && crawlResponse.length() > maxSize) {
			        logger.log(Level.WARNING, "Skipping write of '" + url + "' because it exceeded the defined max size of " + maxSize);
			        return;
			    }

				// add the raw content to the table record
				doc.put(getMongodbParameters().getContentColumnName(), crawlResponse);
			} finally {
				closeStream(replayInputStream);
			}

			this.getDBCollection().insert(doc);
		}
	}

    
	public byte[] serialize(byte[] bytes) {
		if (getMongodbParameters().getSerializer() != null)
			return getMongodbParameters().getSerializer().serialize(bytes);

		return bytes;
	}
	
	@Override
	public void close() throws IOException {
		this._mongoclient.close();
		super.close();
	}
	
	/**
	 * Read the ReplayInputStream and write it to the given BatchUpdate with the given column.
	 *
	 * @param replayInputStream the ris the cell data as a replay input stream
	 * @param streamSize the size
	 * @param curi The {@link CrawlURI} object associated to the given stream. Used to return the byte array in
	 * its proper encoding.
	 * @param encoding Destination encoding
	 *
	 * @return the byte array from input stream
	 *
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	protected String getEncodedStringFromInputStream(final ReplayInputStream replayInputStream, final int streamSize,
	        final CrawlURI curi) throws IOException {

		ByteArrayOutputStream baos = new ByteArrayOutputStream(streamSize);
		try {
			replayInputStream.readFullyTo(baos);
		} finally {
			replayInputStream.close();
		}
		baos.close();

		// Using the byte array and encoding information from the HTTP recorder, reconstruct the string in its
        // native encoding so that we can convert it properly.
		return new String(baos.toByteArray(), curi.getRecorder().getCharset());
	}
    
	protected void closeStream(Closeable c) {
		if (c != null) {
			try {
				c.close();
			} catch(IOException e) {
				logger.log(Level.WARNING, "Exception in closing " + c, e);
			}
		}
	}
	
	/**
     * Get the index (within the string) of the start of the html contents, right after the HTTP response headers.
     * Uses a pretty simple approach of looking for the &lt;!DOCTYPE&gt; or &lt;HTML&gt; tags.
     *
     * @param content
     * @return the index of the start of the contents if found, or -1 otherwise.
     */
    public static int getContentIndex(String content) {
        if (content == null) return -1;
        int tag = content.indexOf("<!DOCTYPE");
        if (tag == -1) tag = content.indexOf("<!doctype");
        if (tag == -1) tag = content.indexOf("<html");
        if (tag == -1) tag = content.indexOf("<HTML");

        return tag;
    }

}