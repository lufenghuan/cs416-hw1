package edu.jhu.cs.damsl.engine.storage.file;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;

import org.jboss.netty.buffer.DirectChannelBufferFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.jhu.cs.damsl.catalog.Schema;
import edu.jhu.cs.damsl.catalog.identifiers.FileId;
import edu.jhu.cs.damsl.catalog.identifiers.PageId;
import edu.jhu.cs.damsl.catalog.identifiers.TableId;
import edu.jhu.cs.damsl.engine.storage.page.Page;
import edu.jhu.cs.damsl.engine.storage.page.PageHeader;
import edu.jhu.cs.damsl.engine.storage.page.factory.HeaderFactory;
import edu.jhu.cs.damsl.utils.hw1.HW1.*;

@CS316Todo
@CS416Todo
public abstract class HeapFile<
                          HeaderType extends PageHeader,
                          PageType extends Page<HeaderType>>
                      implements StorageFile<HeaderType, PageType>
{
  protected static final Logger logger = LoggerFactory.getLogger(HeapFile.class);
  FileId fileId;
  RandomAccessFile file;
  
  TableId relation;
  Schema tupleSchema;
  
  public HeapFile(String fName, Integer pageSize, Long capacity)
      throws FileNotFoundException
  {
    this(fName, pageSize, capacity, null);
  }

  public HeapFile(String fName, Integer pageSize, Long capacity, Schema sch)
      throws FileNotFoundException
  {
    this(fName, pageSize, capacity, sch, null);
  }

  public HeapFile(String fName, Integer pageSz, Long cap, Schema sch, TableId rel)
      throws FileNotFoundException
  {
    initialize(new FileId(fName, pageSz, 0, cap), sch, rel);
  }

  public HeapFile(FileId fId, Schema sch, TableId rel)
      throws FileNotFoundException
  {
    fileId = fId;
    initialize(fId, sch, rel);
  }

  private void initialize(FileId fId, Schema sch, TableId rel)
    throws FileNotFoundException
  {
    fileId = fId;
    file = new RandomAccessFile(fileId.getFile().getAbsolutePath(), "rwd");
    tupleSchema = sch;
    relation = rel;
  }

  public String name() { return fileId.getAddressString(); }
  
  public FileId fileId() { return fileId; }

  public boolean isSorted() { return false; }

  public Schema getSchema() { return tupleSchema; }

  public TableId getRelation() { return relation; }

  public void setRelation(TableId rel) { relation = rel; }

  public int pageSize() { return fileId.pageSize(); }

  public void setPageSize(int p) { fileId.setPageSize(p); }
  
  public int numPages() { return fileId.numPages(); }
  
  public void setNumPages(int n) { fileId.setNumPages(n); }

  long actual_size() {
    long r = -1;
    try { r = file.length(); } catch (IOException e) {
      logger.error("invalid length for {}", fileId.getAddressString());
      e.printStackTrace();
    }
    return r;
  }
  
  public long size() { return numPages()*pageSize(); }
  public long capacity() { return fileId.capacity(); }
  public long remaining() { return capacity() - size(); }


  public void extend(int pageCount) {
    long requestedSize = (numPages()+pageCount)*pageSize();
    if ( requestedSize < capacity() ) {
      try {
        file.setLength(requestedSize);
        setNumPages(numPages()+pageCount);
        logger.info("extending file to {} pages (size in bytes {})", numPages(), size());
      } catch (IOException e) {
        logger.error("could not extend file by {} pages", pageCount);
      }
    } else {
      logger.error("requested extend of {} pages beyond capacity {}", pageCount, capacity());
    }
  }

  public void shrink(int pageCount) {
    if ( pageCount < numPages() ) {
      try {
        file.setLength((numPages()-pageCount) * pageSize());
        setNumPages(numPages()-pageCount);
        logger.info("shrinking file to {} pages (size in bytes {})", numPages(), size());
      } catch (IOException e) {
        logger.error("could not shrink file by {} pages", pageCount);
      }
    } else {
      logger.error("requested shrink of {} pages beyond current {} pages", pageCount, numPages());
    }
  }

  // Initialize an in-memory page with any available schema information from the file.
  public void initializePage(PageType buf) {
    buf.setHeader(buf.getHeaderFactory().getHeader(tupleSchema, buf, (byte) 0));
  }

  // Reads the requested page id into the given buffer.
  // Returns the number of bytes successfully read from the heap file.
  @CS316Todo
  @CS416Todo
  public int readPage(PageType buf, PageId id) {
    int pageNum = id.pageNum();
    if (pageNum >= numPages()) logger.error("readPage():Try to read invalid page index: {} from file",pageNum);
	int pageSize = id.fileId().pageSize();
	int numByte = -1;
	FileChannel fChannel;
	try {
		HeaderFactory<HeaderType> headerFactor = buf.getHeaderFactory();//header factory to read header from buffer
		fChannel = file.getChannel().position(pageNum*pageSize);
		buf.setId(id);
		numByte = buf.setBytes(0, fChannel, pageSize);//copy data from file to buf
		buf.resetReaderIndex();//set read index to the first byte
		buf.writerIndex(pageSize);//set write index to the last byte
		HeaderType h = headerFactor.readHeader(buf);
		buf.setHeader(h);//re-construct header 
	} catch (IOException e) {
		logger.error("Exception occured during read randomAccessFile,{}",e.toString());
	}
	return numByte;
  }

  // Returns the number of bytes written from the page to the heap file.
  @CS316Todo
  @CS416Todo
  public int writePage(PageType p) {
    int r = -1;
	PageId pageId = p.getId();
	int pageSize = pageId.fileId().pageSize();
	int pageIndex = pageId.pageNum();
	
	
	if(pageIndex >=0 && pageIndex < (pageId.fileId().capacity()/pageSize)){//pageNum out of bound
		if (pageIndex >= numPages()){
			setNumPages(pageIndex+1);//update pageNum variable
			extend(pageIndex+1-numPages());
		}
		try {
			FileChannel fChannel = file.getChannel().position(pageIndex*pageSize);
			p.resetReaderIndex();
			p.writerIndex(0);
			p.getHeader().writeHeader(p);//write header to the page
			p.resetReaderIndex();
			p.writerIndex(pageSize);
			r = p.getBytes(0, fChannel, pageSize);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	else {logger.error("writePage(PageType P): pageNum out of bound, pageNum : {}",pageIndex);}
    return r;

  }

  public abstract HeaderFactory<HeaderType> getHeaderFactory();

  protected HeaderType readCurrentPageHeader() throws IOException {
    return getHeaderFactory().readHeaderDirect(file);
  }

  @CS316Todo
  @CS416Todo
  public HeaderType readPageHeader(PageId id) {
    DirectChannelBufferFactory pool = new DirectChannelBufferFactory(pageSize());
	  HeaderType h = null;
	  
	  PageType p = (PageType) pool.getBuffer(pageSize());
	  if(readPage(p, id) > 0 ){
		h  = (HeaderType) new PageHeader(p.getHeader());
	  }
	  return h;
  }

  public String toString() {
    return " pages: " + Integer.toString(numPages()) +
           " size: " + Long.toString(size()) +
           " cap: " + Long.toString(capacity()) +
           " actual: " + Long.toString(actual_size());
  }

}
