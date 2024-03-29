package edu.jhu.cs.damsl.engine.storage.file;

import static org.junit.Assert.*;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import edu.jhu.cs.damsl.catalog.identifiers.FileId;
import edu.jhu.cs.damsl.catalog.identifiers.PageId;
import edu.jhu.cs.damsl.engine.storage.Tuple;
import edu.jhu.cs.damsl.engine.storage.accessor.BufferedPageAccessor;
import edu.jhu.cs.damsl.engine.storage.file.SlottedHeapFile;
import edu.jhu.cs.damsl.engine.storage.file.factory.SlottedStorageFileFactory;
import edu.jhu.cs.damsl.engine.storage.page.SlottedPage;
import edu.jhu.cs.damsl.engine.storage.page.SlottedPageHeader;
import edu.jhu.cs.damsl.engine.storage.page.Page.Permissions;
import edu.jhu.cs.damsl.utils.FileTestUtils;

public class SlottedHeapFileTest {

  private FileTestUtils<SlottedPageHeader, SlottedPage, SlottedHeapFile> ftUtils;
  private BufferedPageAccessor<SlottedPageHeader, SlottedPage, SlottedHeapFile> fileAccessor;

  @Before
  public void setUp() throws Exception {
    ftUtils = new FileTestUtils<SlottedPageHeader, SlottedPage, SlottedHeapFile>(
                new SlottedStorageFileFactory(), true);
    fileAccessor =
      new BufferedPageAccessor<SlottedPageHeader, SlottedPage, SlottedHeapFile>(
        ftUtils.getStorage(), null, Permissions.WRITE, ftUtils.getFile());
  }

  @Test
  public void writeTest() {
    List<Tuple> tuples = ftUtils.getTuples();
    List<SlottedPage> pages = ftUtils.generatePages(fileAccessor, tuples);

    long len = 0;
    for (SlottedPage p : pages) {
      PageId newId = new PageId(fileAccessor.getFileId(), p.getId().pageNum());
      p.setId(newId);
      assertTrue( fileAccessor.getFile().writePage(p)== ftUtils.getPool().getPageSize());
      //System.out.println(fileAccessor.getFile().writePage(p)+" ,"+ftUtils.getPool().getPageSize());
      //System.out.println(fileAccessor.getFile().size()+" , len:"+len);
      assertTrue( fileAccessor.getFile().size() > len );
      len = fileAccessor.getFile().size();
      
      SlottedPage page1 = ftUtils.getPage(fileAccessor);
      fileAccessor.getFile().readPage(page1, newId);
      
      page1.resetReaderIndex();
      page1.resetWriterIndex();
      page1.getHeader().writeHeader(page1);
      page1.resetReaderIndex();
      page1.writerIndex(p.capacity());
      System.out.println("p.offset:"+p.getHeader().getFreeSpaceOffset()+
    		  " ,page1.offset:"+page1.getHeader().getFreeSpaceOffset());
      assertTrue(page1.equals(p)); //the page read from file is equal to page before
    }
    
    
    
    // Write pages again, and ensure no change in file size.
    for (SlottedPage p : pages) {
      PageId newId = new PageId(fileAccessor.getFileId(), p.getId().pageNum());
      p.setId(newId);
      //System.out.println(fileAccessor.getFile().size() == len);
      assertTrue( fileAccessor.getFile().writePage(p)
                        == ftUtils.getPool().getPageSize()
                    && fileAccessor.getFile().size() == len );
    }
  }
  
  @Test
  public void readTest() {
    ftUtils.writeRandomTuples(fileAccessor);

    PageId id = new PageId(fileAccessor.getFileId(),0);
    PageId dummyId = new PageId(new FileId("dummy.dat"),0);
    
    SlottedPage p = ftUtils.getPage(fileAccessor);

    p.setId(dummyId);
    
    //System.out.println("capacity:"+p.capacity());
    
/*
    try {
		RandomAccessFile rf = new RandomAccessFile("/Users/lufenghuan/testFile", "rwd");
		//SlottedPage p = pages.get(0);
		 //p.getBytes(0, rf.getChannel(), p.getId().size());//transfer page to random access file
		
	} catch (IOException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
	*/
    //System.out.println(fileAccessor.getFile().readPage(p, id));
    //System.out.println(ftUtils.getPool().getPageSize());
    assertTrue( fileAccessor.getFile().readPage(p, id)
                  == ftUtils.getPool().getPageSize() );
    PageId pp = p.getId();
    assertTrue( p.getId().equals(id) );
  }
  
  @Test
  public void hashCodeTest(){
	  PageId id1 = new PageId(fileAccessor.getFileId(),0);
	  PageId id2 = new PageId(fileAccessor.getFileId(),0);
	  //PageId
	  assertTrue(id1.equals(id2));
	  assertTrue(id1.hashCode()==id2.hashCode());
	  //FileId
	  assertTrue(id1.fileId().equals(id2.fileId()));
	  assertTrue(id1.fileId().hashCode()==id2.fileId().hashCode());
  }

}
