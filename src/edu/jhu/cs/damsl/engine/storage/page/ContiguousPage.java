package edu.jhu.cs.damsl.engine.storage.page;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.HeapChannelBufferFactory;
import org.omg.CORBA.FREE_MEM;

import edu.jhu.cs.damsl.catalog.Schema;
import edu.jhu.cs.damsl.catalog.identifiers.PageId;
import edu.jhu.cs.damsl.engine.storage.Tuple;
import edu.jhu.cs.damsl.engine.storage.iterator.page.ContiguousPageIterator;
import edu.jhu.cs.damsl.engine.storage.page.factory.PageFactory;
import edu.jhu.cs.damsl.engine.storage.page.factory.HeaderFactory;
import edu.jhu.cs.damsl.engine.storage.page.factory.PageHeaderFactory;
import edu.jhu.cs.damsl.utils.hw1.HW1.*;

@CS316Todo
@CS416Todo
public class ContiguousPage extends Page<PageHeader> {

  public static final HeaderFactory<PageHeader> headerFactory 
    = new PageHeaderFactory();

  // Constructor variants.
  public ContiguousPage(Integer id, ChannelBuffer buf, Schema sch, byte flags) {
    super(id, buf, sch, flags);
  }

  public ContiguousPage(PageId id, ChannelBuffer buf, Schema sch, byte flags) {
    super(id, buf, sch, flags);
  }

  public ContiguousPage(Integer id, ChannelBuffer buf, Schema sch) {
    this(id, buf, sch, (byte) 0);
  }
  
  public ContiguousPage(PageId id, ChannelBuffer buf, Schema sch) {
    this(id, buf, sch, (byte) 0);
  }

  public ContiguousPage(Integer id, ChannelBuffer buf, byte flags) {
    this(id, buf, null, flags);
  }
  
  public ContiguousPage(PageId id, ChannelBuffer buf, byte flags) {
    this(id, buf, null, flags);
  }


  // Construct a contiguous page without initializing a header.
  public ContiguousPage(ContiguousPage p) throws InvalidPageException
  {
    super(p);
    if ( header.getTupleSize() <= 0 ) throw new InvalidPageException();
  }

  // Factory accessors
  @Override
  public HeaderFactory<PageHeader> getHeaderFactory() {
    return headerFactory;
  }

  @Override
  public ContiguousPageIterator iterator() {
    return new ContiguousPageIterator(getId(), this);
  }
  
  @CS416Todo
  /**
   * validTupleBoundary checks whether the offset is aligned 
   * with a tuple boundary (depending on the fill direction and the header).
   * @param offset
   * @return
   */
  protected boolean validTupleBoundary(short offset) { 
	  if(header.filledBackward()){
		  return offset > header.getFreeSpaceOffset();
	  }
	  else{
		  return offset < header.getFreeSpaceOffset() 
				  && offset >= header.headerSize;
	  }
  }
  
  @CS416Todo
  /**
   * checks whether the byte region defined by 
   * offset and length is within the data segment of the
   * page (again dependent on fill direction and header size), 
   * as well as checking if the offset is at a valid tuple boundary.
   * @param offset
   * @param length
   * @return 
   */
  public boolean isValidData(short offset, short length) { 
	  if(!isValidOffset(offset) || length < 0)
		  return false;
	  
	  return ( (offset+length) <= header.bufCapacity  
			  && (offset >=header.headerSize) );
  }
  
  @CS416Todo
  public boolean isValidOffset(short offset) { 
  	if(header.tupleSize == PageHeader.VARIABLE_LENGTH)
  		throw new UnsupportedOperationException("varaible length unsupport!");
  	if(header.filledBackward())
  		return (offset >= header.freeSpaceOffset
		  		&& (offset + header.tupleSize) <= header.bufCapacity);
  	else
  		return (offset >= header.headerSize
  		&& (offset + header.tupleSize) <= header.freeSpaceOffset);
  }

  @CS416Todo
  protected ChannelBuffer getBuffer(short offset, short length) {
	if(!isValidData(offset, length)) {
		logger.warn("CountiguousPage: getBuffer(offset,length) invalid data range offset:{}, length{}",offset,length);
		return null;
	}
	ChannelBuffer buf = HeapChannelBufferFactory.getInstance().getBuffer(length);
	buf.setBytes(0, this, offset, length);
	buf.readerIndex(0);
	buf.writerIndex(length);
    return buf;
  }

  @CS416Todo
  /**
   * 
   * @param offset 
   * @param length tupleSize tuple size NOTE:  contain tupleHeader
   * @return A tuple of length from offset
   */
  public Tuple getTuple(short offset, short length) {
	ChannelBuffer buf = getBuffer(offset, length);
    return Tuple.getTuple(buf, length);
  }

  @CS416Todo
  public Tuple getTuple(short offset) {
  short length = header.tupleSize;
	if(length== PageHeader.VARIABLE_LENGTH){
		length = this.getShort(offset);
	}
	return getTuple(offset, length);
  }
  
  @CS416Todo
  protected boolean putBuffer(ChannelBuffer buf, short length) {
	if(header.getFreeSpace() < length){
		logger.error("Pgae do not have eought space to hold {} byte",length);
		return false;
	}
	int offset = header.getFreeSpaceOffset();
	if(header.filledBackward())
		offset = header.getFreeSpaceOffset()-length;

	this.setBytes(offset, buf, length);
	header.useSpace(length);
    return true;
  }

  @CS416Todo
  @Override
  public boolean putTuple(Tuple t, short tupleSize) {
	if(header.getFreeSpace() < tupleSize){
		logger.error("Pgae do not have eought space to hold {} byte tuple",tupleSize);
		return false;
	}
	int offset = header.getFreeSpaceOffset();
	if(header.filledBackward())
		offset = header.getFreeSpaceOffset()-tupleSize;
	this.setBytes(offset, t, 0, tupleSize);
	header.useSpace(tupleSize);
	return true;

  }

  @CS416Todo
  @Override
  public boolean putTuple(Tuple t) {
	int tupleSize = t.size();
    if(header.getFreeSpace() < tupleSize){
		logger.error("Pgae do not have eought space to hold {} byte tuple",tupleSize);
		return false;
	}
    return putTuple(t, (short)tupleSize);
  
  }

  @CS416Todo
  /**
   * Shift data. NOTE: does NOT check boundary
   * @param offset the offset want to shift, negative for deletion, positive for insertion
   * @param shift length of data need after shift
   */
  protected void shiftBuffer(short offset, short shift) {
	  int shiftStart = 0;
	  int shiftEnd = 0;
	  int shiftLen = 0;
	  int shiftDes = 0;
	  if(header.filledBackward()){
		  shiftStart = header.getFreeSpaceOffset();
		  shiftEnd = offset;
		  if(shift > 0)
			  shiftDes = shiftStart - shift;
		  else
			  shiftDes = shiftEnd - shift;
	  }
	  else {
		  shiftStart = offset;
		  shiftEnd = header.getFreeSpaceOffset();
		  if(shift > 0)
			  shiftDes = shiftStart + shift;
		  else
			  shiftDes = shiftEnd - shift;
	  }
	  shiftLen = shiftEnd - shiftStart;
	  ChannelBuffer tempBuf = HeapChannelBufferFactory.getInstance().getBuffer(shiftLen);
	  tempBuf.setBytes(0, this, header.getFreeSpaceOffset(), shiftLen); //copy to temp buffer
	  this.setBytes(shiftDes, tempBuf,0 , shiftLen); //copy to page
  }
  
  // Add a buffer at the given offset, shifting existing data.
  @CS416Todo
  /**
   * insert buffer to specific offset. NOTE:only allow to insert at area that has data
   * @param offset offset to insert
   * @param buf data channel buffer
   * @param length length of data to insert 
   * @return if the insert is successful
   */
  protected boolean insertBuffer(short offset, ChannelBuffer buf, short length) {
	if(!isValidOffset(offset)) {
		logger.warn("Invalid offset:{}",offset);
		return false;
	}
	if(header.getFreeSpace() < length){return false;}

	if(!validTupleBoundary(offset)){
		return false;
	}
	shiftBuffer(offset, length);
	this.setBytes(offset, buf, 0, length); 
	header.useSpace(length); //update freespaceoffset 
	return true;
  }
  
  @CS416Todo
  public boolean insertTuple(short offset, Tuple t, short tupleSize) {
	return insertBuffer(offset, t, tupleSize);
  }
  
  @CS416Todo
  public boolean insertTuple(short offset, Tuple t) {
	return insertBuffer(offset, t, (short)t.size());
  }

  @CS416Todo
  /**
   * Not sure what the three param means
   * @param offset
   * @param removeLength
   * @param dataLength
   */
  protected void removeBuffer(short offset, short removeLength, short dataLength) {
	  
  }

  @CS416Todo
  public boolean removeTuple(short offset, short length) {
	if(!isValidData(offset, length)) return false;
	shiftBuffer(offset, (short)(-1*length));
	header.freeSpace(length);
    return true;
  }

  @CS416Todo
  public boolean removeTuple(short offset) {
    return removeTuple(offset, header.tupleSize) ;
  }

  @CS416Todo
  public void clearTuples() {
	  header.resetHeader();
  }

}
