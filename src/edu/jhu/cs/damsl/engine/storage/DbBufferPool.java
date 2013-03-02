package edu.jhu.cs.damsl.engine.storage;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import edu.jhu.cs.damsl.catalog.Defaults;
import edu.jhu.cs.damsl.catalog.Defaults.SizeUnits;
import edu.jhu.cs.damsl.catalog.identifiers.FileId;
import edu.jhu.cs.damsl.catalog.identifiers.PageId;
import edu.jhu.cs.damsl.catalog.identifiers.TableId;
import edu.jhu.cs.damsl.engine.storage.BaseBufferPool;
import edu.jhu.cs.damsl.engine.storage.StorageEngine;
import edu.jhu.cs.damsl.engine.storage.file.StorageFile;
import edu.jhu.cs.damsl.engine.storage.page.Page;
import edu.jhu.cs.damsl.engine.storage.page.PageHeader;
import edu.jhu.cs.damsl.utils.LRUCache;
import edu.jhu.cs.damsl.utils.hw1.HW1.*;

@CS316Todo
@CS416Todo
public class DbBufferPool<HeaderType extends PageHeader,
                          PageType   extends Page<HeaderType>,
                          FileType   extends StorageFile<HeaderType, PageType>>
                extends BaseBufferPool<HeaderType, PageType>
{
  // Page cache, tracking used buffer pool pages in access order.
  //LinkedHashMap<PageId, PageType> pageCache;
  LRUCache<PageId, PageType> pageCache; //use LRUCache
  
  StorageEngine<HeaderType, PageType, FileType> storage;
 

  // Buffer pool statistics.
  long numRequests;
  long numHits;
  long numEvictions;
  long numFailedEvictions;

  public DbBufferPool(StorageEngine<HeaderType, PageType, FileType> e,
                      Integer bufferSize, SizeUnits bufferUnit,
                      Integer pageSize, SizeUnits pageUnit)
  {
    super(e.getPageFactory(), bufferSize, bufferUnit, pageSize, pageUnit);
    logger.debug("Size of pool: {} pages",getNumFreePages());
    logger.debug("File contaion {} pages",Defaults.getDefaultFileSize()/getPageSize());
    storage = e;
   //pageCache = new LinkedHashMap<PageId, PageType>();
    pageCache = new LRUCache<PageId,PageType>(super.numPages);
    numRequests = numHits = numEvictions = numFailedEvictions = 0;
  }

  /**
   * Cached page access.
   * This should check if the page is already in the buffer pool, 
   * and issue a read request to the file manager to pull the page into the
   * buffer pool as necessary. Pages should be evicted as needed if the
   * cache is full.
   *
   * @param id the page id of the page being requested
   * @return an in-memory page reflecting the contents of the on-disk page
   */
  @CS316Todo
  @CS416Todo
  public PageType getPage(PageId id) {
	numRequests++;
	PageType page = this.pageCache.get(id); // a page in buffer pool
	if(page == null){//requested page does not exist in the buffer pool
		if(freePages.isEmpty()){//not free page, must evict
			page = evictPage();
		}
		else {
			page = getPageIfReady();
		}
		if(storage.fileMgr.readPage(page, id) == null) {
			releasePage(page);//File Manager cannot find such pageId, so put the page back to freePage
		}
		else{
			pageCache.put(id, page);
		}
	}else {numHits++;}
	return page;

  }

  /**
   * Return a cached page to the free list without flushing it, regardless
   * of whether the page is dirty.
   *
   * @param id the page to flush
   */
  @CS316Todo
  @CS416Todo
  public void discardPage(PageId id) {
	  PageType page = pageCache.remove(id);
	  if(page == null) logger.error("discardPage provide a invalid PageId {}", id);
	  releasePage(page);

  }

  /**
   * Cache eviction policy, yielding the page that has just been evicted for
   * reuse, without putting it back on the free page list.
   *
   * @return the page evicted under the replacement policy
   */
  @CS316Todo
  @CS416Todo
  PageType evictPage() {
	//current implementation no concurrent
    Iterator<Entry<PageId,PageType>> itr=pageCache.LRUCacheIterator();
	PageType p = null;
	if(itr.hasNext()) {
		p=pageCache.remove(itr.next().getKey());
		if(p.getHeader().isDirty())
			storage.fileMgr.writePage(p); //write back
		//p.getHeader().resetHeader();
		numEvictions++;
	}
	return p;
  }
  
  /**
    * Flush a cached page to disk if it is dirty, returning it to the free list.
    */
  @CS316Todo
  @CS416Todo
  public void flushPage(PageId id) {
	  PageType p = pageCache.remove(id);
	  if(p == null) logger.error("discardPage provide a invalid PageId {}", id);
	  if(p.isDirty()){//dirty, write back
		  storage.fileMgr.writePage(p);
		  PageType deleteP = pageCache.remove(p.getId());
		  releasePage(deleteP);
	  }
  }
  
  /**
    * Flush all dirty pages currently in the buffer pool to disk.
    */
  @CS316Todo
  @CS416Todo
  public void flushPages() {
	  Iterator<Entry<PageId,PageType>> itr=pageCache.LRUCacheIterator();
	  LinkedList<PageType> flushs = new LinkedList <PageType>();
	  while(itr.hasNext()){
		  Entry<PageId,PageType> e = itr.next();
		  PageType p = e.getValue();
		  if(p.isDirty()) {
			  storage.fileMgr.writePage(p);
			  //releasePage(pageCache.remove(e.getKey()));//concurrent modify excetion
			  flushs.add(p);
		  }
	  }
	  for(PageType p :flushs){
		 PageType deleteP = pageCache.remove(p.getId());
		 releasePage(deleteP);
	  }
  }

  /**
    * Buffered data access for adding data to the given relation.
    * This method should support requests for a page with at least
    * <code>requestedSpace</code> bytes free. If no such page is already
    * in the buffer pool, a request should be made on files backing
    * the relation in the file manager. The file manager may need to
    * extend its current files, and any page returned should be added
    * to the buffer pool.
    *
    * @param rel the relation for which we are requesting a page with
    *            sufficient free space
    * @param requestedSpace the amount of space requested
    * @return the page id of a page satisfying the space requirements
    */
  @CS316Todo
  @CS416Todo
  public PageId getWriteablePage(TableId rel, short requestedSpace) {
	List<FileId> fileIds =  rel.getFiles();
	//Iterator<Entry<PageId,PageType>> cachePageItr = pageCache.LRUCacheIterator();
	HashSet<PageId> visited = new HashSet<PageId> ();
	//ArrayList<PageId> visited = new ArrayList<PageId> ();
	//PageType p = null;
	if(fileIds.size() > 0){
		FileId lastFid = fileIds.get(fileIds.size()-1);
		int num = lastFid.numPages();
		if(num > 0) {
			PageId lastPageId = new PageId (lastFid,num-1);
			PageType lastPage = pageCache.get(lastPageId);
			if(lastPage!=null && lastPage.getHeader().isSpaceAvailable(requestedSpace)){
				return lastPageId;}
			else visited.add(lastPageId);
		}
	}
	//linear scan not efficent
//	while(cachePageItr.hasNext()){
//		p = cachePageItr.next().getValue();
//		if(fileIds.contains(p.getId().fileId())){
//			visited.add(p.getId());
//			if(p.getHeader().isSpaceAvailable(requestedSpace)){ 
//				return p.getId();}//find in buffer pool
//		}
//	}
	return storage.fileMgr.getWriteablePage(rel, requestedSpace, visited);//use null, only consider the last page in last file
  }
  
  /**
   * clear buffer pool, flush the modified page
   */
  public void clearPool(){
  	flushPages();
  	
  	Iterator<Entry<PageId,PageType>> itr=pageCache.LRUCacheIterator();
	  LinkedList<PageId> toDelete = new LinkedList <PageId>();
	  while(itr.hasNext()){
		  Entry<PageId,PageType> e = itr.next();
		  PageId p = e.getKey();
		  toDelete.add(p);		  
	  }
	  
	  for(PageId id :toDelete){
	  	discardPage(id);
	  }
  }

  public String toString() {
    double hitRate = Long.valueOf(numHits).doubleValue() / Long.valueOf(numRequests).doubleValue();

    String r = "hits: "              + (numRequests > 0? Double.toString(hitRate) : "<>") +
               " requests: "         + Long.toString(numRequests) +
               " hits: "             + Long.toString(numHits) +
               " evictions: "        + Long.toString(numEvictions) +
               " failed evictions: " + Long.toString(numFailedEvictions) + "\n";

    r += "free pages: " + Integer.toString(getNumFreePages()) +
         " total pages: " + Integer.toString(getNumPages());

    return r;
  }

}
