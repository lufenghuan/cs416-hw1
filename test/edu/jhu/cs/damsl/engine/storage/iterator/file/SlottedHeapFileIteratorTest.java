package edu.jhu.cs.damsl.engine.storage.iterator.file;

import static org.junit.Assert.*;

import java.io.FileNotFoundException;
import java.util.List;
import java.util.ListIterator;

import org.junit.Before;
import org.junit.Test;

import edu.jhu.cs.damsl.engine.storage.Tuple;
import edu.jhu.cs.damsl.engine.storage.accessor.HeapFileAccessor;
import edu.jhu.cs.damsl.engine.storage.file.SlottedHeapFile;
import edu.jhu.cs.damsl.engine.storage.file.factory.SlottedStorageFileFactory;
import edu.jhu.cs.damsl.engine.storage.iterator.file.heap.SlottedHeapFileIterator;
import edu.jhu.cs.damsl.engine.storage.page.SlottedPage;
import edu.jhu.cs.damsl.engine.storage.page.SlottedPageHeader;
import edu.jhu.cs.damsl.utils.FileTestUtils;

public class SlottedHeapFileIteratorTest {

  private FileTestUtils<SlottedPageHeader, SlottedPage, SlottedHeapFile> ftUtils;
  private List<Tuple> tuples;
  
  @Before
  public void setUp() {
    try {
      ftUtils = new FileTestUtils<SlottedPageHeader, SlottedPage, SlottedHeapFile>(
                  new SlottedStorageFileFactory(), false);
      tuples = ftUtils.getTuples();
      
      HeapFileAccessor<SlottedPageHeader, SlottedPage, SlottedHeapFile> accessor =
        new HeapFileAccessor<SlottedPageHeader, SlottedPage, SlottedHeapFile>(
          ftUtils.getPool(), ftUtils.getFile());
      
      ftUtils.writeTuples(accessor, tuples);
    } catch (FileNotFoundException e) { e.printStackTrace(); }
  }
  
  @Test
  public void iterateTest() {
	SlottedHeapFile f = ftUtils.getFile();//one page in file
	assertTrue(f!=null);
    SlottedHeapFileIterator iterator =
      new SlottedHeapFileIterator(ftUtils.getPool(), f);

    ListIterator<Tuple> tupleIt = tuples.listIterator();

    while ( tupleIt.hasNext() ) {
      assertTrue ( iterator.hasNext() );
      Tuple expected = tupleIt.next();
      Tuple t = iterator.next();
      assertTrue ( expected.equals(t) ); 
    }
    
    assertTrue ( !iterator.hasNext() );
  }
}
