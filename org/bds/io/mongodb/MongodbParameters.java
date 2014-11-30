/*
 *  This file is part of the Heritrix web crawler (crawler.archive.org).
 *  MongoDB storage has been added for big data crawler to store crawled Internet data. 
 *  
 *  @email herman_cn@163.com
 *  @date 2014-10-21
 */

package org.bds.io.mongodb;

public class MongodbParameters {
	
	/** DEFAULT OPTIONS **/
	public static final int DEFAULT_MONGODB_PORT = 27017;
	public static final boolean REMOVE_MISSING_PAGES = true;
	public static final int DEFAULT_MAX_CONTENT_SIZE_IN_BYTES = 16*1024*1024; // The maximum size of doc in MongoDB is 16M

	// Writing will continue when some error occurred.
	//TODO: use this flag
	public static final boolean CONTINUE_ON_ERROR = true;
	public static final int BULK_DOC_NUMBER = 100; // Insert 100 docs in a batch.
	
	// "content" logical grouping
	public static final String CONTENT_PREFIX = "content";
	public static final String HEADERS_COLUMN_NAME = "headers";
	public static final String CONTENT_COLUMN_NAME = "raw_data";

	// "curi" logical grouping
	public static final String CURI_PREFIX = "curi";
	public static final String IP_COLUMN_NAME = "ip";
	public static final String PATH_FROM_SEED_COLUMN_NAME = "path-from-seed";
	public static final String IS_SEED_COLUMN_NAME = "is-seed";
	public static final String VIA_COLUMN_NAME = "via";
	public static final String URL_COLUMN_NAME = "url";
	public static final String REQUEST_COLUMN_NAME = "request";
	public static final String PROCESSED_AT_NAME = "processed_at";

	
	/** ACTUAL OPTIONS INITIALIZED TO DEFAULT **/
	private String host = "";
	private int port = DEFAULT_MONGODB_PORT;
	private String database = "";
	private String collection = "";
	private String user = "";
	private String password = "";
	private Serializer serializer = null;
	
	private boolean removeMissingPages = REMOVE_MISSING_PAGES;
	private int defaultMaxContentSize = DEFAULT_MAX_CONTENT_SIZE_IN_BYTES;

	private boolean separateHeaders = true;
	private String timeZone = null;
	private String contentPrefix = CONTENT_PREFIX;
	private String headersColumnName = contentPrefix + ":" + HEADERS_COLUMN_NAME;
	private String contentColumnName = contentPrefix + ":" + CONTENT_COLUMN_NAME;

	private String curiPrefix = CURI_PREFIX;
	private String ipColumnName = curiPrefix + ":" + IP_COLUMN_NAME;
	private String pathFromSeedColumnName = curiPrefix + ":" + PATH_FROM_SEED_COLUMN_NAME;
	private String isSeedColumnName = curiPrefix + ":" + IS_SEED_COLUMN_NAME;
	private String viaColumnName = curiPrefix + ":" + VIA_COLUMN_NAME;
	private String urlColumnName = curiPrefix + ":" + URL_COLUMN_NAME;
	private String requestColumnName = curiPrefix + ":" + REQUEST_COLUMN_NAME;
	private String processedAtColumnName = curiPrefix + ":" + PROCESSED_AT_NAME;
	private int bulkDocNumber = BULK_DOC_NUMBER;

	
	public String getHost() {
		if (host.isEmpty())
			throw new RuntimeException("A host was never set for this object. " +
			"Define one before trying to access it.");
		return host;
	}
	public void setHost(String host) {
		this.host = host;
	}
	public int getPort() {
		return port;
	}
	public void setPort(int port) {
		this.port = port;
	}
	public String getDatabase() {
		if (database.isEmpty())
			throw new RuntimeException("A database was never set for this object. " +
			"Define one before trying to access it.");
		return database;
	}
	public void setDatabase(String database) {
		this.database = database;
	}
	public String getCollection() {
		if (collection.isEmpty())
			throw new RuntimeException("A collection was never set for this object. " +
			"Define one before trying to access it.");
		return collection;
	}
	public void setCollection(String collection) {
		this.collection = collection;
	}
	public String getUser() {
		return user;
	}
	public void setUser(String user) {
		this.user = user;
	}
	public String getPassword() {
		return password;
	}
	public void setPassword(String password) {
		this.password = password;
	}
	
	public Serializer getSerializer() {
		return serializer;
	}
	public void setSerializer(Serializer serializer) {
		this.serializer = serializer;
	}
	
	public boolean isRemoveMissingPages() {
		return removeMissingPages;
	}
	public void setRemoveMissingPages(boolean removeMissingPages) {
		this.removeMissingPages = removeMissingPages;
	}
	public int getDefaultMaxContentSizeInBytes() {
	    return defaultMaxContentSize;
	}
	public void getDefaultMaxContentSizeInBytes(int defaultMaxContentSize) {
	    this.defaultMaxContentSize = defaultMaxContentSize;
	}
	
	public String getContentPrefix() {
		return contentPrefix;
	}
	public void setContentPrefix(String contentPrefix) {
		this.contentPrefix = contentPrefix;
	}
	public String getHeadersColumnName() {
        return headersColumnName;
    }
    public void setHeadersColumnName(String headersColumnName) {
        this.headersColumnName = headersColumnName;
    }
	public String getContentColumnName() {
		return contentColumnName;
	}
	public void setContentColumnName(String contentColumnName) {
		this.contentColumnName = contentColumnName;
	}
	public String getCuriPrefix() {
		return curiPrefix;
	}
	public void setCuriPrefix(String curiPrefix) {
		this.curiPrefix = curiPrefix;
	}
	public String getIpColumnName() {
		return ipColumnName;
	}
	public void setIpColumnName(String ipColumnName) {
		this.ipColumnName = ipColumnName;
	}
	public String getPathFromSeedColumnName() {
		return pathFromSeedColumnName;
	}
	public void setPathFromSeedColumnName(String pathFromSeedColumnName) {
		this.pathFromSeedColumnName = pathFromSeedColumnName;
	}
	public String getIsSeedColumnName() {
		return isSeedColumnName;
	}
	public void setIsSeedColumnName(String isSeedColumnName) {
		this.isSeedColumnName = isSeedColumnName;
	}
	public String getViaColumnName() {
		return viaColumnName;
	}
	public void setViaColumnName(String viaColumnName) {
		this.viaColumnName = viaColumnName;
	}
	public String getUrlColumnName() {
		return urlColumnName;
	}
	public void setUrlColumnName(String urlColumnName) {
		this.urlColumnName = urlColumnName;
	}
	public String getRequestColumnName() {
		return requestColumnName;
	}
	public void setRequestColumnName(String requestColumnName) {
		this.requestColumnName = requestColumnName;
	}
	public String getProcessedAtColumnName() {
		return processedAtColumnName;
	}
	public void setProcessedAtColumnName(String processedAtColumnName) {
		this.processedAtColumnName = processedAtColumnName;
	}
	public int getBulkDocNumber() {
		return bulkDocNumber;
	}
	public void setBulkDocNumber(int bulkDocNumber) {
		this.bulkDocNumber = bulkDocNumber;
	}
	public boolean isSeparateHeaders() {
		return separateHeaders;
	}
	public void setSeparateHeaders(boolean separateHeaders) {
		this.separateHeaders = separateHeaders;
	}
	public String getTimeZone() {
		return timeZone;
	}
	public void setTimeZone(String timeZone) {
		this.timeZone = timeZone;
	}
}
