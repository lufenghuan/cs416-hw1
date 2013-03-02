package edu.jhu.cs.damsl.engine.storage.page;

import java.util.BitSet;
import java.util.List;

import org.jboss.netty.buffer.ChannelBuffer;

import edu.jhu.cs.damsl.catalog.Schema;
import edu.jhu.cs.damsl.engine.storage.Tuple;
import edu.jhu.cs.damsl.language.core.types.Type;
import edu.jhu.cs.damsl.utils.hw1.HW1.*;

@CS416Todo
/**
 * Only implement fixed length tuple
 * @author lufenghuan
 *
 */
public class PAXPageHeader extends PageHeader {

	protected short recordCapacity;
	protected short numRecord;
	protected short attributeNum;
	protected short[] attributeLen;
	protected short[] offsets;
	protected BitSet exist;


	public PAXPageHeader(Schema sch, ChannelBuffer buf, byte flags) {
		 super(flags,
	        (sch == null? VARIABLE_LENGTH
	                      :  (sch.getTupleSize().shortValue())),
	        (short) buf.capacity());
		assert(sch != null && sch.getTupleSize() > 0);
		
		recordCapacity = (short) (Math.ceil( bufCapacity-super.headerSize-(Short.SIZE>>3)*2) * 8 /33);
				
				
		List<Type> types = sch.getTypes();
		attributeNum = (short) types.size();
		attributeLen = new short [attributeNum];
		for(int i = 0; i < attributeNum; i++){
			attributeLen[i] = types.get(i).getSize().shortValue();
		}
		headerSize = getHeaderSize();
		exist = new BitSet(recordCapacity);
				resetHeader();
	}

	@Override
	public void writeHeader(ChannelBuffer buf){
		super.writeHeader(buf);
		buf.writeShort(attributeNum);
		buf.writeShort(attributeNum);
		for(short s : attributeLen){
			buf.writeShort(s);
		}
		for(short s : offsets){
			buf.writeShort(s);
		}
		buf.writeBytes(exist.toByteArray());
	}
	@Override
	public void resetHeader(){
		exist.set(0, recordCapacity, false);
		numRecord = 0;

		for(int i = 0; i < recordCapacity; i++){
			if( i== 0){
				offsets[i] = headerSize;
			}
			else{
				offsets[i] = (short) (headerSize + attributeLen[i-1]/tupleSize);
			}
		}
	}

	@Override
	public short getHeaderSize(){
		return (short) (super.getHeaderSize()+
				(Short.SIZE>>3)*(2+4*recordCapacity)+exist.toByteArray().length);
	}

	//Return the available space in the page, in bytes. 
	public short getFreeSpace() {
		return (short) (bufCapacity - tupleSize*numRecord);
	}

	public short getDataOffset(){
		throw new UnsupportedOperationException("Cannot get dataOffset from PAXPage");		
	}
	
	public short getPrevTupleOffset(short offset, short length){
		throw new UnsupportedOperationException("Cannot get getPrevTupleOffset from PAXPage");		
	}
	
	public short getNextTupleOffset(short offset) {
		throw new UnsupportedOperationException("Cannot get getNextTupleOffset from PAXPage");
  }
  
  // For forward filling, length must be that of the current tuple.
  // For backward filling, length must be that of the next tuple.
  public short getNextTupleOffset(short offset, short length) {
  	throw new UnsupportedOperationException("Cannot get getNextTupleOffset from PAXPage");
  }
  
  // Free space index management.
  void advanceForward(short length) {
  	throw new UnsupportedOperationException("Cannot get advanceForward from PAXPage");
  }
  
  void advanceBackward(short length) {
  	throw new UnsupportedOperationException("Cannot get advanceBackward from PAXPage");
  }

  public void useSpace(short length) {
    numRecord += (short) (length/tupleSize);
    
  }

  public void freeSpace(short length) {
  	numRecord -= (short) (length/tupleSize);
  }

  public String toString() {
    return "ts: " + tupleSize
            + ", hs: " + headerSize
            + ", fso: " + freeSpaceOffset
            + ", cap: " + bufCapacity;
  }
}