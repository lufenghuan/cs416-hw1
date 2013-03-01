package edu.jhu.cs.damsl.engine.storage.iterator.file.header;

import java.util.Iterator;

import edu.jhu.cs.damsl.catalog.identifiers.FileId;
import edu.jhu.cs.damsl.catalog.identifiers.PageId;
import edu.jhu.cs.damsl.engine.storage.file.StorageFile;
import edu.jhu.cs.damsl.engine.storage.page.ContiguousPage;
import edu.jhu.cs.damsl.engine.storage.page.PageHeader;
import edu.jhu.cs.damsl.utils.hw1.HW1.*;

@CS416Todo
public class ContiguousFileHeaderIterator
              implements StorageFileHeaderIterator<PageHeader>
{
  StorageFile<PageHeader, ContiguousPage> file;
  FileId fileId;
  int filePages;
  PageId currentPageId;
  
  public ContiguousFileHeaderIterator(StorageFile<PageHeader, ContiguousPage> f) {
	  file = f;
	  fileId = f.fileId();
	  reset();
  }
  
  public FileId getFileId() { return fileId; }

  public PageId getPageId() { return currentPageId; }
  
  public void reset() {
	  filePages = file.numPages();
	  currentPageId = (filePages > 0 ? new PageId(fileId,0) :null);
  }
  
  public void nextPageId() {
  	if(currentPageId != null){
  		currentPageId = (currentPageId.pageNum()+1 < filePages ?
  				new PageId(fileId,currentPageId.pageNum()+1) : null);
  	}
  }

  public boolean hasNext() { 
  	return (currentPageId == null ? false:
  			currentPageId.pageNum() < filePages);
  }

  public PageHeader next() { 
  	PageHeader h = null;
  	if(currentPageId == null) return h;
  	h = file.readPageHeader(currentPageId);
  	nextPageId();
  	return h; 
  }

  public void remove() {
  	throw new UnsupportedOperationException("cannot remove page headers");
  }
}
