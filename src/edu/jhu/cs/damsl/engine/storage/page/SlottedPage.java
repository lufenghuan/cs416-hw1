package edu.jhu.cs.damsl.engine.storage.page;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.buffer.HeapChannelBufferFactory;

import edu.jhu.cs.damsl.catalog.Schema;
import edu.jhu.cs.damsl.catalog.identifiers.PageId;
import edu.jhu.cs.damsl.engine.storage.Tuple;
import edu.jhu.cs.damsl.engine.storage.iterator.page.SlottedPageIterator;
import edu.jhu.cs.damsl.engine.storage.page.SlottedPageHeader.Slot;
import edu.jhu.cs.damsl.engine.storage.page.factory.HeaderFactory;
import edu.jhu.cs.damsl.engine.storage.page.factory.SlottedPageHeaderFactory;
import edu.jhu.cs.damsl.utils.hw1.HW1.*;

@CS316Todo
@CS416Todo
public class SlottedPage extends Page<SlottedPageHeader> {
  
  public static final HeaderFactory<SlottedPageHeader> headerFactory 
    = new SlottedPageHeaderFactory();

  // Commonly used constructors.
  public SlottedPage(Integer id, ChannelBuffer buf) {
    super(id, buf, PageHeader.FILL_BACKWARD);
  }

  public SlottedPage(PageId id, ChannelBuffer buf) {
    super(id, buf, PageHeader.FILL_BACKWARD);
  }

  // Constructor variants
  public SlottedPage(Integer id, ChannelBuffer buf, Schema sch, byte flags) {
    super(id, buf, sch, flags);
  }

  public SlottedPage(PageId id, ChannelBuffer buf, Schema sch, byte flags) {
    super(id, buf, sch, flags);
  }

  public SlottedPage(Integer id, ChannelBuffer buf, Schema sch) {
    this(id, buf, sch, PageHeader.FILL_BACKWARD);
  }
  
  public SlottedPage(PageId id, ChannelBuffer buf, Schema sch) {
    this(id, buf, sch, PageHeader.FILL_BACKWARD);
  }

  public SlottedPage(Integer id, ChannelBuffer buf, byte flags) {
    this(id, buf, null, (byte) (flags | PageHeader.FILL_BACKWARD));
  }
  
  public SlottedPage(PageId id, ChannelBuffer buf, byte flags) {
    this(id, buf, null, (byte) (flags | PageHeader.FILL_BACKWARD));
  }

  // Construct a slotted page without initializing a header.
  public SlottedPage(SlottedPage p) { super(p); }

  // Factory accessors.
  @Override
  public HeaderFactory<SlottedPageHeader> getHeaderFactory() { 
    return headerFactory;
  }
  
  // Header accessors.
  @Override
  public SlottedPageHeader getHeader() { return header; }

  @Override
  public void setHeader(SlottedPageHeader hdr) {  header = hdr; }

  @Override
  public void readHeader() {
    header = getHeaderFactory().readHeader(this);
  }
  
  // Tuple accessors.
  @Override
  public SlottedPageIterator iterator() {
    return new SlottedPageIterator(getId(), this);
  }

  @CS316Todo
  @CS416Todo
  /**
   * Get a  tuple form given slotIndex
   * @param slotIndex given slot index
   * @param tupleSize tuple size NOTE:  contain tupleHeader
   * @return a tuple form given slotIndex
   */
  public Tuple getTuple(int slotIndex, int tupleSize) {
	if(!header.isValidTuple(slotIndex)){
		logger.error("invalid tuple index {}",slotIndex);
		return null;
	}
	Slot s = header.getSlot(slotIndex);
	/*
	ChannelBuffer buf = 
			HeapChannelBufferFactory.getInstance().getBuffer(tupleSize);
	buf.writeInt(tupleSize);
	buf.writeBytes(this, s.offset, s.length);
	*/
	Tuple t = Tuple.emptyTuple(tupleSize-Tuple.headerSize, header.tupleSize == PageHeader.VARIABLE_LENGTH);
	t.writeBytes(this, s.offset, s.length);

	return t;
  }
  
  @CS316Todo
  @CS416Todo
  /**
   * Given the index of a slot in the page.
   * @param slotIndex
   * @return
   */
  public Tuple getTuple(int slotIndex) {
	  if(!header.isValidTuple(slotIndex)){
		  logger.error("invalid tuple index {}",slotIndex);
		  return null;
	  }
	  short len = header.getSlotLength(slotIndex);
	  return getTuple(slotIndex, len+Tuple.headerSize);
  }

  // Adds a tuple the to start of the free space block.
  @CS316Todo
  @CS416Todo
  /**NOTE: This implementation does not write tuple header to the page
   * @param t tuple
   * @param tupleSize size of the tuple contaion tuple header
   * @return if the put is successful
   */
  public boolean putTuple(Tuple t, short tupleSize) {
	int index = header.getNextSlot();
    if(header.useNextSlot((short) (tupleSize-Tuple.headerSize.shortValue())) == 1 ){
    	
    	int writeLen = 0;
		if(this.header.tupleSize == PageHeader.VARIABLE_LENGTH) writeLen = tupleSize -Tuple.headerSize;
		else writeLen = this.header.tupleSize;
		this.setBytes(header.getSlotOffset(index), t, t.readerIndex(),writeLen);
		return true;
	}
	return false;
  }
 
  @Override
  @CS316Todo
  @CS416Todo
  /**
   *  NOTE: This implementation does not write tuple header to the page
   */
  public boolean putTuple(Tuple t) {
	return putTuple(t, (short)t.size());
  }

  
  @CS316Todo
  @CS416Todo
  /**
   *  Inserts a tuple at the given slot in this page, overwriting the existing
   *  entry for fixed length tuples.
   * @param t
   * @param tupleSize
   * @param slotIndex
   * @return if the insert is successful
   */
  public boolean insertTuple(Tuple t, short tupleSize, int slotIndex) {
	if(header.useSlot(slotIndex, tupleSize)){
		//t.readBytes(this,header.getSlotOffset(slotIndex),tupleSize);
		int writeLen = 0;
		if(this.header.tupleSize == PageHeader.VARIABLE_LENGTH) writeLen = tupleSize -Tuple.headerSize;
		else writeLen = this.header.tupleSize;
		this.setBytes(header.getSlotOffset(slotIndex), t, t.readerIndex(),writeLen);
		return true;
	}
	return false;

  }
  
  @CS316Todo
  @CS416Todo
  public boolean insertTuple(Tuple t, int slotIndex) {
    return insertTuple(t, (short)t.size(),slotIndex);
  }

  // Zeroes out the contents of the given slot.
  @CS316Todo
  @CS416Todo
  protected void clearTuple(int slotIndex) {
	  header.setSlot(slotIndex, (short)-1, header.getSlotLength(slotIndex));
  }

  // Removes the tuple at the given slot in this page, zeroing the tuple data.
  @CS316Todo
  @CS416Todo
  public boolean removeTuple(int slotIndex) {
	if(header.isValidTuple(slotIndex)){
		clearTuple(slotIndex);
		return true;
	}
    return false;
  }

  @CS316Todo
  @CS416Todo
  public void clearTuples() {header.resetHeader();}

}
