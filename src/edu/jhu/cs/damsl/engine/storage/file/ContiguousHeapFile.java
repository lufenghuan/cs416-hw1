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
import edu.jhu.cs.damsl.engine.storage.iterator.file.buffered.BufferedContiguousFileIterator;
import edu.jhu.cs.damsl.engine.storage.iterator.file.buffered.BufferedSlottedFileIterator;
import edu.jhu.cs.damsl.engine.storage.iterator.file.header.ContiguousFileHeaderIterator;
import edu.jhu.cs.damsl.engine.storage.iterator.file.header.SlottedFileHeaderIterator;
import edu.jhu.cs.damsl.engine.storage.iterator.file.header.StorageFileHeaderIterator;
import edu.jhu.cs.damsl.engine.storage.iterator.file.heap.ContiguousHeapFileIterator;
import edu.jhu.cs.damsl.engine.storage.iterator.file.heap.SlottedHeapFileIterator;
import edu.jhu.cs.damsl.engine.storage.iterator.page.StorageIterator;
import edu.jhu.cs.damsl.engine.storage.page.ContiguousPage;
import edu.jhu.cs.damsl.engine.storage.page.Page;
import edu.jhu.cs.damsl.engine.storage.page.PageHeader;
import edu.jhu.cs.damsl.engine.storage.page.SlottedPage;
import edu.jhu.cs.damsl.engine.storage.page.SlottedPageHeader;
import edu.jhu.cs.damsl.engine.storage.page.factory.HeaderFactory;
import edu.jhu.cs.damsl.utils.hw1.HW1.*;

@CS416Todo
public class ContiguousHeapFile extends HeapFile<PageHeader, ContiguousPage> {
	StorageEngine<PageHeader, ContiguousPage, ContiguousHeapFile> engine;

  public ContiguousHeapFile(StorageEngine<PageHeader, ContiguousPage, ContiguousHeapFile> e,
		  String fName, Integer pageSize, Long capacity)
      throws FileNotFoundException
  {
    this(e,fName, pageSize, capacity, null);
  }

  public ContiguousHeapFile(StorageEngine<PageHeader, ContiguousPage, ContiguousHeapFile> e,
		  String fName, Integer pageSz, Long capacity, Schema sch)
      throws FileNotFoundException
  {
    super(fName, pageSz, capacity, sch);
    engine = e;
  }

  public ContiguousHeapFile( StorageEngine<PageHeader, ContiguousPage, ContiguousHeapFile> e,
		  FileId id, Schema sch, TableId rel)
      throws FileNotFoundException
  {
    super(id, sch, rel);
    engine = e;
  }

  @CS416Todo
  public StorageIterator iterator() { 
	  return new ContiguousHeapFileIterator(engine.getBufferPool(), this);
  }

  @CS416Todo
  public StorageFileIterator<PageHeader, ContiguousPage> heap_iterator() { 
	  return new ContiguousHeapFileIterator(engine.getBufferPool(), this);
  }

  @CS416Todo
  public StorageFileIterator<PageHeader, ContiguousPage>
  buffered_iterator(TransactionId txn, Page.Permissions perm) { 
    return new BufferedContiguousFileIterator(engine, txn, perm, this);

  }


  // Returns a direct iterator over on-disk page headers.
  @CS416Todo
  public StorageFileHeaderIterator<PageHeader> header_iterator() {
    return new ContiguousFileHeaderIterator(this);

  }
  
  @CS416Todo
  public HeaderFactory<PageHeader> getHeaderFactory() {
    return ContiguousPage.headerFactory;
  }
}