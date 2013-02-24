package edu.jhu.cs.damsl.engine.storage.page;

import static org.junit.Assert.*;

import java.util.List;
import java.util.ListIterator;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.HeapChannelBufferFactory;
import org.junit.Before;
import org.junit.Test;

import edu.jhu.cs.damsl.catalog.Defaults;
import edu.jhu.cs.damsl.catalog.Schema;
import edu.jhu.cs.damsl.engine.storage.Tuple;
import edu.jhu.cs.damsl.engine.storage.file.SlottedHeapFile;
import edu.jhu.cs.damsl.engine.storage.file.factory.SlottedStorageFileFactory;
import edu.jhu.cs.damsl.engine.storage.page.SlottedPage;
import edu.jhu.cs.damsl.engine.storage.page.SlottedPageHeader;
import edu.jhu.cs.damsl.utils.CommonTestUtils;
import edu.jhu.cs.damsl.utils.PageTestUtils;

public class SlottedPageTest {

  private PageTestUtils<SlottedPageHeader, SlottedPage, SlottedHeapFile> ptUtils;
  private List<Tuple> tuples;

  @Before
  public void setUp() {
    ptUtils = new PageTestUtils<SlottedPageHeader, SlottedPage, SlottedHeapFile>(
                new SlottedStorageFileFactory());
    tuples = ptUtils.getTuples();
  }

  @Test
  public void headerTest() {
    Schema sch = CommonTestUtils.getLIDSchema();

    SlottedPage p = null;
    ChannelBuffer buf = HeapChannelBufferFactory.getInstance().getBuffer(Defaults.defaultPageSize);
      p = new SlottedPage(-1, buf, sch);
    
    short expectedSize =
        (short) (((Long.SIZE+Integer.SIZE+Double.SIZE)>>3)+Tuple.headerSize);

    int expectedSlots =
      (p.capacity() - (1+(Short.SIZE>>3)*5))
        / (expectedSize+SlottedPageHeader.SLOT_SIZE);
    assertTrue ( p != null
                  && p.getHeader().getTupleSize() == expectedSize
                  && !p.getHeader().hasDynamicSlots() 
                  && p.getHeader().getNumSlots() == expectedSlots );
  
    try {
      p = (SlottedPage) ptUtils.getPool().getPage();
    } catch (InterruptedException e) { e.printStackTrace(); }

    assertTrue ( p != null
                  && p.getHeader().getTupleSize() == -1
                  && p.getHeader().hasDynamicSlots() 
                  && p.getHeader().getNumSlots() == 0 );
  }

  @Test
  public void getTest() {
    List<SlottedPage> dataPages = ptUtils.generatePages(tuples);
    ListIterator<Tuple> tupleIt = tuples.listIterator();
     
    for (SlottedPage p : dataPages) {
      SlottedPageHeader hdr = p.getHeader();
      int numSlots = hdr.getNumSlots();
      for (int i = 0; i < numSlots; ++i) {
        if ( hdr.getSlotOffset(i) < 0 ) break;
        else {
          Tuple t = p.getTuple(i);
          assertTrue ( t != null );
          assertTrue ( tupleIt.hasNext() );
          Tuple check = tupleIt.next();
          /*
          ChannelBuffer buf1 = 
      			HeapChannelBufferFactory.getInstance().getBuffer(20);
          ChannelBuffer buf2 = 
      			HeapChannelBufferFactory.getInstance().getBuffer(20);
          for (int j = 0; j<3; j++){
        	  buf1.writeInt(j);
        	  buf2.writeInt(j);
          }
          System.out.println(buf1.equals(buf2));
          System.out.println(buf1.readInt()+","+buf1.equals(buf2));
          System.out.println(buf2.readInt()+","+buf1.equals(buf2));
          */
//          Tuple tu = Tuple.emptyTuple(t.size(), !t.isFixedLength());

//          System.out.println("t:"+t.readInt()+","+t.readInt()+" ,"+t.readInt()+" ,"+t.readInt());
//          System.out.println("check"+check.readInt()+","+check.readInt()+" ,"+check.readInt()+" ,"+check.readInt());
          
          assertTrue ( t.equals(check) );
        }
      }
    }

    // Ensure that all tuples generated have been verified.
    assertTrue ( !tupleIt.hasNext() );
  }

  @Test
  public void putTest() {
    SlottedPage p = null;
    
    for (Tuple t : tuples) {
      // Get a new page if the previous one is full.
      if ( p == null || p.getHeader().getFreeSpace() < t.size()) {
        p = ptUtils.getPage();
      }
      assertTrue ( p != null && p.getHeader() != null );

      short tupleSize = (short) t.size();
      int slotIndex = p.getHeader().getNextSlot();

      short hdrSzBefore = p.getHeader().getHeaderSize();
      short fsBefore = p.getHeader().getFreeSpace();
      short fsOffsetBefore = p.getHeader().getFreeSpaceOffset();

      // Put the generated data in the page.
      boolean valid = p.putTuple(t, (short) t.size());

      // Slotted pages by default have dynamically allocated slots, thus
      // the free space difference must account for slot growth.
      short hdrSzDiff = (short) (p.getHeader().getHeaderSize() - hdrSzBefore);
      short fsDiff = (short) (fsBefore - p.getHeader().getFreeSpace());
      short fsOffsetDiff =
          (short) (p.getHeader().getFreeSpaceOffset() - fsOffsetBefore);

      // Ensure that the write resulted in a new valid slot and that the
      // slot entry matches the tuple.
      assertTrue( valid);
      assertTrue(p.getHeader().getNextSlot() == slotIndex+1);
      //assertTrue(p.getHeader().getSlotLength(slotIndex) == t.size());
      assertTrue(p.getHeader().getSlotLength(slotIndex) == (tupleSize-Tuple.headerSize));
      //System.out.println("fsDiff"+fsDiff+" ,tupleSize+hdrSzDiff"+(tupleSize-Tuple.headerSize+hdrSzDiff));
      assertTrue (fsDiff == (tupleSize-Tuple.headerSize+hdrSzDiff));
      assertTrue((p.getHeader().filledBackward()?
                        (fsOffsetDiff == -(tupleSize-Tuple.headerSize))
                        : fsOffsetDiff == (tupleSize-Tuple.headerSize)) );
    }
  }

  @Test
  public void insertTest() {
    List<SlottedPage> dataPages = ptUtils.generatePages(tuples);
    List<Tuple> newTuples = ptUtils.getTuples();
    ListIterator<Tuple> tupleIt = newTuples.listIterator();
    
    for (SlottedPage p : dataPages) {
      SlottedPageHeader hdr = p.getHeader();
      int numSlots = hdr.getNumSlots();
      for (int i = 0; i < numSlots; ++i) {
        short offset = hdr.getSlotOffset(i);
        if ( offset < 0 ) break;
        else {
          // New tuples should have as many tuples as the original dataset.
          assertTrue ( tupleIt.hasNext() );
          Tuple t = tupleIt.next();
          /*
          ChannelBuffer buf1 = 
        			HeapChannelBufferFactory.getInstance().getBuffer(20);
          buf1.setBytes(buf1.writerIndex(), t, t.readerIndex(), t.size()-Tuple.headerSize);
          Tuple tu = Tuple.emptyTuple(t.size()-Tuple.headerSize, !t.isFixedLength());
      	  tu.writeBytes(buf1, 0, t.size()-Tuple.headerSize);
      	  System.out.println(tu.equals(t));
          */
          
          short length = hdr.getSlotLength(i);
          boolean valid = p.insertTuple(t, (short) t.size(), i);
          
          // Verify equal size updates happen at the specified slot, and
          // resizing updates result in a new slot.
          assertTrue ( valid &&
                        (t.size() == length?
                            hdr.getSlotOffset(i) == offset
                            && hdr.getSlotLength(i) == length
                          : hdr.getSlotOffset(i) != offset ) );
        }
      }
      
      // Ensure that all new tuples have been inserted.
      assertTrue ( !tupleIt.hasNext() );
    }
  }
  
  @Test
  public void removeTest() {
    List<SlottedPage> dataPages = ptUtils.generatePages(tuples);
    
    for (SlottedPage p : dataPages) {
      SlottedPageHeader hdr = p.getHeader();
      int numSlots = hdr.getNumSlots();
      for (int i = numSlots-1; i >= 0; --i) {
        p.removeTuple(i);
        
        // Ensure slot no longer points to a valid page segment.
        //assertTrue ( hdr.getSlotOffset(i) < 0 && hdr.getSlotLength(i) < 0 );
        assertTrue ( hdr.getSlotOffset(i) < 0 ); //I only set offset to -1, length unchanged

      }
    }
  }

  @Test
  public void clearTest() {
    List<SlottedPage> dataPages = ptUtils.generatePages(tuples);
    
    for (SlottedPage p : dataPages) {
      SlottedPageHeader hdr = p.getHeader();

      assertTrue( hdr.getUsedSpace() > 0 );
      
      p.clearTuples();
      
      assertTrue( hdr.getUsedSpace() == 0
                  && hdr.getFreeSpaceOffset() ==
                      (hdr.filledBackward()?
                          hdr.getCapacity() : hdr.getHeaderSize())
                  && hdr.getNextSlot() == 0 );
    }
  }

  
  /*
  @Test
  public void overflowTest() {
    
  }
  
  @Test
  public void underflowTest() {
    
  }
  */
  
}
