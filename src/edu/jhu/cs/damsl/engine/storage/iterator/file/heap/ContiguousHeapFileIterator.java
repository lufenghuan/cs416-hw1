package edu.jhu.cs.damsl.engine.storage.iterator.file.heap;

import edu.jhu.cs.damsl.catalog.identifiers.PageId;
import edu.jhu.cs.damsl.engine.EngineException;
import edu.jhu.cs.damsl.engine.storage.DbBufferPool;
import edu.jhu.cs.damsl.engine.storage.accessor.PageFileAccessor;
import edu.jhu.cs.damsl.engine.storage.file.ContiguousHeapFile;
import edu.jhu.cs.damsl.engine.storage.page.ContiguousPage;
import edu.jhu.cs.damsl.engine.storage.page.PageHeader;
import edu.jhu.cs.damsl.engine.storage.page.SlottedPage;
import edu.jhu.cs.damsl.utils.hw1.HW1.*;

@CS416Todo
public class ContiguousHeapFileIterator
                extends HeapFileIterator<PageHeader, ContiguousPage, ContiguousHeapFile>
{
  public ContiguousHeapFileIterator(DbBufferPool<PageHeader, ContiguousPage, ContiguousHeapFile> pool,
                                 ContiguousHeapFile file)
  {
    super(pool, file);
  }

  @Override
  public void nextValidTuple() {
  	PageId id = null;
  	while ( (current == null || !current.hasNext())
  			&& (id = nextPageId()) != null ){
  		ContiguousPage p = null;
  		try {
  			p = paged.getPage(id);
  		} catch (EngineException e) {
  			logger.warn("could not read page {}", id);
  		}

  		current = (p == null? null : p.iterator());
  		if ( current == null && p != null ) {
  			// Invoke release before moving on to the next page. It is left
  			// to the reader to determine what actually happens on release.
  			paged.releasePage(p);
  		}
  	}//end of while
  }
  

}
