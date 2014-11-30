/*
 *  This file is part of the Heritrix web crawler (crawler.archive.org).
 *  MongoDB storage has been added for big data crawler to store crawled Internet data. 
 *  
 *  @email herman_cn@163.com
 *  @date 2014-10-21
 */

package org.bds.io.mongodb;

public interface Serializer {

	/**
     * Implement if you want to serialize bytes in a custom manner.
     * @param bytes
     * @return serialized bytes
     */
    public byte[] serialize(byte[] bytes);
}
