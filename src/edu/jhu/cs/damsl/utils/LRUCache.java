package edu.jhu.cs.damsl.utils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;


/*
public class LRUCache<K,V> extends LinkedHashMap<K, V> {
  long capacity;

  public LRUCache(long capacity) {
    this.capacity = capacity;
  }

  @Override
  protected boolean removeEldestEntry(Map.Entry<K, V> entry) {
    return size() > capacity;
  }

  private static final long serialVersionUID = 419771866081560106L;

}
 */

/**
 * An LRU cache, based on <code>LinkedHashMap</code>.
 *
 * <p>
 * This cache has a fixed maximum number of elements (<code>cacheSize</code>).
 * If the cache is full and another entry is added, the LRU (least recently used) entry is dropped.
 *
 */
public class LRUCache<K,V> {

	private static final float   hashTableLoadFactor = 0.75f;

	private LinkedHashMap<K,V>   map;
	private int                  cacheSize;

	/**
	 * Creates a new LRU cache.
	 * @param cacheSize the maximum number of entries that will be kept in this cache.
	 */
	public LRUCache (int cacheSize) {
		this.cacheSize = cacheSize;
		int hashTableCapacity = (int)Math.ceil(cacheSize / hashTableLoadFactor) + 1;
		map = new LinkedHashMap<K,V>(hashTableCapacity, hashTableLoadFactor, true) {
			// (an anonymous inner class)
			private static final long serialVersionUID = 419771866081560106L;
			@Override protected boolean removeEldestEntry (Map.Entry<K,V> eldest) {
				return size() > LRUCache.this.cacheSize; }}; 
	}

	/**
	 * Retrieves an entry from the cache.<br>
	 * The retrieved entry becomes the MRU (most recently used) entry.
	 * @param key the key whose associated value is to be returned.
	 * @return    the value associated to this key, or null if no value with this key exists in the cache.
	 */
	public  V get (K key) {
		return map.get(key); }

	/**
	 * Adds an entry to this cache.
	 * The new entry becomes the MRU (most recently used) entry.
	 * If an entry with the specified key already exists in the cache, it is replaced by the new entry.
	 * If the cache is full, the LRU (least recently used) entry is removed from the cache.
	 * @param key    the key with which the specified value is to be associated.
	 * @param value  a value to be associated with the specified key.
	 */
	public  void put (K key, V value) {
		map.put (key, value); }

	/**
	 * Clears the cache.
	 */
	public  void clear() {
		map.clear(); }

	/**
	 * Returns the number of used entries in the cache.
	 * @return the number of entries currently in the cache.
	 */
	public  int usedEntries() {
		return map.size(); }

	/**
	 * Returns a <code>Collection</code> that contains a copy of all cache entries.
	 * @return a <code>Collection</code> with a copy of the cache content.
	 */
	public  Collection<Map.Entry<K,V>> getAll() {
		return new ArrayList<Map.Entry<K,V>>(map.entrySet()); }
	
	/**
	 * return a iterator that is based on <code>LinkedHashMap<\code>
	 * @return a iterator that is based on <code>LinkedHashMap<\code>
	 */
	public Iterator<Entry<K,V>> LRUCacheIterator(){
		 return map.entrySet().iterator();
	}
	/**
	 * remove a value by given k.
	 * @param k key
	 * @return the remove V mapping with k, null if not exist
	 */
	public V remove (K k){
		return map.remove(k);
	}

} // end class LRUCache

