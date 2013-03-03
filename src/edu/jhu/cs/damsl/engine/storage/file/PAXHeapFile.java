package edu.jhu.cs.damsl.engine.storage.file;

import java.io.FileNotFoundException;
import java.io.IOException;

import edu.jhu.cs.damsl.catalog.Schema;
import edu.jhu.cs.damsl.catalog.identifiers.FileId;
import edu.jhu.cs.damsl.catalog.identifiers.PageId;
import edu.jhu.cs.damsl.catalog.identifiers.TableId;
import edu.jhu.cs.damsl.catalog.identifiers.TransactionId;
import edu.jhu.cs.damsl.engine.storage.StorageEngine;
import edu.jhu.cs.damsl.engine.storage.iterator.file.StorageFileIterator;
import edu.jhu.cs.damsl.engine.storage.iterator.file.buffered.BufferedPAXFileIterator;
import edu.jhu.cs.damsl.engine.storage.iterator.file.buffered.BufferedSlottedFileIterator;
import edu.jhu.cs.damsl.engine.storage.iterator.file.header.PAXFileHeaderIterator;
import edu.jhu.cs.damsl.engine.storage.iterator.file.header.StorageFileHeaderIterator;
import edu.jhu.cs.damsl.engine.storage.iterator.file.heap.PAXHeapFileIterator;
import edu.jhu.cs.damsl.engine.storage.iterator.file.heap.SlottedHeapFileIterator;
import edu.jhu.cs.damsl.engine.storage.iterator.page.StorageIterator;
import edu.jhu.cs.damsl.engine.storage.page.Page;
import edu.jhu.cs.damsl.engine.storage.page.PageHeader;
import edu.jhu.cs.damsl.engine.storage.page.PAXPage;
import edu.jhu.cs.damsl.engine.storage.page.PAXPageHeader;
import edu.jhu.cs.damsl.engine.storage.page.SlottedPage;
import edu.jhu.cs.damsl.engine.storage.page.SlottedPageHeader;
import edu.jhu.cs.damsl.engine.storage.page.factory.HeaderFactory;
import edu.jhu.cs.damsl.engine.storage.page.factory.PAXPageFactory;
import edu.jhu.cs.damsl.utils.hw1.HW1.*;

@CS416Todo
public class PAXHeapFile extends HeapFile<PAXPageHeader, PAXPage> {
	StorageEngine<PAXPageHeader, PAXPage, PAXHeapFile> engine;

  public PAXHeapFile(StorageEngine<PAXPageHeader, PAXPage, PAXHeapFile> e,
  		String fName, Integer pageSize, Long capacity)
      throws FileNotFoundException
  {
  	
    this(e,fName, pageSize, capacity, null);
  }

  public PAXHeapFile(StorageEngine<PAXPageHeader, PAXPage, PAXHeapFile> e,
  		String fName, Integer pageSz, Long capacity, Schema sch)
      throws FileNotFoundException
  {
    super(fName, pageSz, capacity, sch);
    engine = e;
  }

  public PAXHeapFile(FileId id, Schema sch, TableId rel)
      throws FileNotFoundException
  {
    super(id, sch, rel);
  }
  public PAXHeapFile(StorageEngine<PAXPageHeader, PAXPage, PAXHeapFile> e,
      FileId id, Schema sch, TableId rel)
throws FileNotFoundException
{
super(id, sch, rel);
engine = e;
}
  @CS416Todo
  public StorageIterator iterator() { 
  	return new PAXHeapFileIterator(engine.getBufferPool(), this);
  	
  }

  @CS416Todo
  public StorageFileIterator<PAXPageHeader, PAXPage> heap_iterator() { 
  	return new PAXHeapFileIterator(engine.getBufferPool(), this);
  }

  @CS416Todo
  public StorageFileIterator<PAXPageHeader, PAXPage>
  buffered_iterator(TransactionId txn, Page.Permissions perm) { 
  	return new BufferedPAXFileIterator(engine, txn, perm, this);
  }


  // Returns a direct iterator over on-disk page headers.
  @CS416Todo
  public StorageFileHeaderIterator<PAXPageHeader> header_iterator() {
    return new PAXFileHeaderIterator(this);
  }
  
  @CS416Todo
  public HeaderFactory<PAXPageHeader> getHeaderFactory() {
    return PAXPage.headerFactory;
  }
}