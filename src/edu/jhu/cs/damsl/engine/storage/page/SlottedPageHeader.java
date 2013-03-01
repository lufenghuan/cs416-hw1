package edu.jhu.cs.damsl.engine.storage.page;

import java.io.DataInput;
import java.io.IOException;
import java.util.ArrayList;

import org.jboss.netty.buffer.ChannelBuffer;

import edu.jhu.cs.damsl.catalog.Defaults;
import edu.jhu.cs.damsl.catalog.Schema;
import edu.jhu.cs.damsl.engine.storage.Tuple;
import edu.jhu.cs.damsl.utils.hw1.HW1.*;

@CS316Todo
@CS416Todo
public class SlottedPageHeader extends PageHeader {

  public static final short SLOT_SIZE = (Short.SIZE>>3)*2;
  public static final short INVALID_SLOT = -1;
  
  //origional number of slots
  protected short origionNum;
  //slot directory 
  protected short slotIdx; //free slot search marker 
  protected short numSlots;
  protected ArrayList<Slot> slots = null; 
  
  public class Slot {
    public short offset;
    public short length;
    
    public Slot() { offset = -1; length = -1; }
    public Slot(short off, short len) { offset = off; length = len; }
  }

  public SlottedPageHeader(ChannelBuffer buf) {
    this((byte) 0x0, (short) -1, buf);
  }

  public SlottedPageHeader(Schema sch, ChannelBuffer buf, byte flags) {
//    this(flags,
//         (short) (sch == null? -1 : (sch.getTupleSize()+Tuple.headerSize)),
//         buf);
    
    this(flags,
            (short) (sch == null? -1 : (sch.getTupleSize())),
            buf);
  }

  public SlottedPageHeader(byte flags, short tupleSize, ChannelBuffer buf)
  {
    this(flags, tupleSize, (short) buf.capacity());
  }

  public SlottedPageHeader(byte flags, short tupleSize, short bufCapacity)
  {
    super(flags, tupleSize, bufCapacity);
    short numSlots = 
      (tupleSize <= 0? Integer.valueOf(-1).shortValue() : 
        Integer.valueOf(
          (bufCapacity - (super.getHeaderSize()+((Short.SIZE>>3)*2)))
              / (tupleSize + SLOT_SIZE)).shortValue());
    
    resetHeader(numSlots, (short) 0);
  }

  public SlottedPageHeader(byte flags, short tupleSz, short bufCapacity,
                           short numSlots, short slotIdx)
  {
    super(flags, tupleSz, bufCapacity);
    resetHeader(numSlots, slotIdx);
  }
  
  public SlottedPageHeader(PageHeader h, short numSlots, short slotIdx) {
    super(h);
    resetHeader(numSlots, slotIdx);
  }
  /**
	 * Resets this header, altering the number of slots maintained in
	 * the page directory, and setting the free slot search marker to
	 * the given slot index.
	 * 
	 * I change numSlots to int 
	 */
	@CS316Todo
	@CS416Todo
	public void resetHeader(short numSlots, short slotIdx) {
		//System.out.println(numSlots+" ,"+slotIdx);
		if(slotIdx > numSlots) {this.numSlots = slotIdx;this.slotIdx=slotIdx;}
		else if(numSlots == -1 && slotIdx == -1){this.numSlots = 0;slotIdx=(short)0;}
		else {this.numSlots = numSlots;this.slotIdx = slotIdx;}
		if(slots == null){
			//System.out.println(this.numSlots+" ,"+slotIdx);
			this.slots = new ArrayList<Slot>(this.numSlots);
			for(int i = 0; i < this.numSlots; i++){
				Slot s = new Slot((short)-1, (short)-1);
				slots.add(s);
			}
			this.headerSize = getHeaderSize();
			super.resetHeader();//rest freeSpaceOffset
		}
		else{
			int size =  slots.size();
							
			for(Slot s:slots){
				s.length = -1;
				s.offset = -1;
			}
			if(numSlots > size){
				for(int i= size; i<numSlots; i++){
					Slot s = new Slot((short)-1,(short)-1);
					slots.add(s);
				}
			}
			this.headerSize = getHeaderSize();
			super.resetHeader();//rest freeSpaceOffset

		}
		
	}

	/**
	 * Resets the header, clearing the current state of the slot directory,
	 * and setting the free slot search marker to the first slot.
	 */
	@CS316Todo
	@CS416Todo
	public void resetHeader() {
		if(tupleSize != PageHeader.VARIABLE_LENGTH)
			resetHeader(numSlots,(short)0);
		else resetHeader((short)0, (short)0);
	}

	/**
	 * Writes this header to the buffer backing this page.
	 */
	@CS316Todo
	@CS416Todo
	@Override
	public void writeHeader(ChannelBuffer buf) {
		//function fron SlottedPageHeaderFactory
		/*
	   public SlottedPageHeader readHeader(ChannelBuffer buf) {
     PageHeader h = pageHeaderFactory.readHeader(buf);
     short slotCapacity = buf.readShort(); 
     short slotIndex = slotCapacity > 0? buf.readShort() : -1;
     short actualSlots = slotCapacity <= 0? buf.readShort() : slotCapacity;

     SlottedPageHeader r = new SlottedPageHeader(h, slotCapacity, slotIndex);

     short offset = 0;
     for ( short i = 0; i < actualSlots; ++i) {
     Slot s = r.new Slot(buf.readShort(), buf.readShort());
     r.setSlot(i, s);
     short noffset = (short) (s.offset + s.length);
     offset = offset > noffset? offset : noffset;
    }
    if ( offset > 0 ) { r.useSpace((short) (offset-r.getFreeSpaceOffset())); }
    return r;
     }
		 */
		super.writeHeader(buf);
		//System.out.println("SlottedPageHeader.super: short:"+tupleSize+" , short:"+bufCapacity+" ,freeSpace:"+freeSpaceOffset);
		short j;
		if(this.tupleSize < 0){
			//variable tuple size
			buf.writeShort(numSlots);
			buf.writeShort(slotIdx);
			j = slotIdx;
			//System.out.println("SlottedPageHeader: short:"+-1+" , short:"+slotIdx+" ,j:"+j);
		} 
		else{
			//fixed tuple size
			buf.writeShort(numSlots);
			buf.writeShort(slotIdx);
			j = (short) numSlots;
			//System.out.println("SlottedPageHeader: short:"+numSlots+" , short:"+slotIdx+" ,j:"+j);
		} 
		for(short i = 0; i < j; i++){
			Slot s = slots.get(i);
			buf.writeShort(s.offset);
			buf.writeShort(s.length);
		}
	}

	/**
	 * Returns the size of the header, in bytes.
	 */
	@CS316Todo
	@CS416Todo
	@Override
	public short getHeaderSize() { 
			return  (short) (super.getHeaderSize()+(Short.SIZE>>3)*2+numSlots*SLOT_SIZE);
	}


	/**
	 * Returns whether the page associated with this header has the
	 * given amount of space available for use.
	 */  
	@CS316Todo
	@CS416Todo
	@Override
	public boolean isSpaceAvailable(short size) { 
//		if (!isValidTupleSize(size)) return false; //not valid tuple size
//		else if (numSlots == slotIdx) return false;
//		return getFreeSpace() >= size; //if there is enough free space
		
		if (!isValidTupleSize(size)) return false; //not valid tuple size
		else if (numSlots == slotIdx) return (getFreeSpace() > (size+SLOT_SIZE));//dynamic growth
		return getFreeSpace() >= size; //if there is enough free space
	}
	/**
	 * Returns the number of slots in the header.
	 */
	@CS316Todo
	@CS416Todo
	public int getNumSlots() { return this.numSlots;}

	/**
	 * Returns the slot at the given index in the slot directory.
	 */
	@CS316Todo
	@CS416Todo
	public Slot getSlot(int index) { 
		if(isValidSlot(index)) return slots.get(index); 
		else{ 
			logger.warn("getSlot: invalid slot index {}",index);
			return null;
		}
		
	}

	/**
	 * Expands the slot directory to ensure that it can contain
	 * the given slot at the specified index. Blank slots should
	 * be added to the slot directory as needed.
	 */
	@CS316Todo
	@CS416Todo
	public void growSlots(int index, Slot s) {
		int preSize = numSlots;
		int newSize = index + 1;
		slots.ensureCapacity(newSize);
		numSlots = (short)newSize;
		for(int i = preSize;i < newSize; i++ ){
			Slot newS = new Slot ((short)-1,(short)-1);
			slots.add(newS);
		}
		slots.set(index, s);
		useSpace(s.length);
		this.headerSize = getHeaderSize();
		slotIdx = (short)(index + 1);

	}

	/**
	 * Sets the slot at the given index.
	 */
	@CS316Todo
	@CS416Todo
	public void setSlot(int index, Slot s) throws IndexOutOfBoundsException {
		slots.set(index, s);
	}

	/**
	 * Sets the slot at the given index to point to the given offset
	 * and length within the page.
	 */
	@CS316Todo
	@CS416Todo
	public void setSlot(int index, short offset, short length) {
		Slot s = new Slot(offset, length);
		setSlot(index, s);
	}

	/**
	 * Returns the page offset of the i'th slot.
	 * NOTE: need to check the if the slot is out of bound before call
	 */
	@CS316Todo
	@CS416Todo
	public short getSlotOffset(int slot) { return slots.get(slot).offset; }

	/**
	 * Returns the tuple size of the i'th slot.
	 * NOTE: need to check the if the slot is out of bound before call
	 */
	@CS316Todo
	@CS416Todo
	public short getSlotLength(int slot) { return slots.get(slot).length; }

	@CS316Todo
	@CS416Todo
	public short getRequiredSpace(int slotIndex, short reqSpace) { 
		// TO DO 
		
		return -1; 
	}

	/**
	 * Returns the index of the next free slot. Return -1 if not found
	 * 
	 */
	@CS316Todo
	@CS416Todo
	public int getNextSlot() { 
		if(slotIdx > numSlots) return -1;
		return slotIdx;
	}

	/**
	 * Advances the free slot index.
	 */
	@CS316Todo
	@CS416Todo  
	void advanceSlot() {slotIdx++;}

	/**
	 * Indicates whether this header can resize its slot directory.
	 */
	@CS316Todo
	@CS416Todo
	public boolean hasDynamicSlots() { 
		return (filledBackward() && tupleSize == PageHeader.VARIABLE_LENGTH); }

	/**
	 * Indicates whether this slot at the given index exists.
	 */
	@CS316Todo
	@CS416Todo
	public boolean isValidSlot(int index) { 
		return index >= 0 && index < numSlots ;
	}

	/**
	 * Returns whether the requested index contains a valid tuple (i.e.
	 * a non-negative offset and length).
	 */
	@CS316Todo
	@CS416Todo
	public boolean isValidTuple(int index) { 
		return isValidSlot(index)
				&& slots.get(index).offset >= 0 && slots.get(index).length > 0; 
	}

	/**
	 * Returns whether a tuple of the given size can be written at
	 * the given slot index.
	 */
	@CS316Todo
	@CS416Todo
	public boolean isValidPut(int slotIndex, short size) { 
		if(isValidSlot(slotIndex)){//out bound 
			logger.error("invalid slot index {}, outbound",slotIndex);
			return false;
		}
		Slot s = slots.get(slotIndex);
		if( s.offset < -1 && s.length >= size)//previously deleted slot 
			return false;
		if( s.offset > 0 && s.length>=size)//exit slot
			return true;
		if(s.offset < 0 && s.length < 0 && getFreeSpace() >= size)//unused slot
			return true;
		return false; 
	}

	/**
	 * Returns whether a tuple of the given size can be written to the
	 * underlying page. First consider it can append to next empty slot, if no
	 * try append
	 */
	@CS316Todo
	@CS416Todo
	public boolean isValidAppend(short size) { 
		if (!isValidTupleSize(size)) return false; //not valid tuple size
		else if (numSlots == slotIdx) return false;
		return getFreeSpace() >= size; //if there is enough free space
		
//		if(isSpaceAvailable(size)) return true;
//		else if (getFreeSpace() > (SLOT_SIZE+size)) return true;
//		return false;
		/*
		if(slotIdx == numSlots) return false;
		else if (getFreeSpace() >= size) return true;
		else return false;
		*/
	}

	/**
	 * Uses the slot at the given index, and returns whether the operation
	 * succeeded.
	 */
	@CS316Todo
	@CS416Todo
	public boolean useSlot(int slotIndex, short tupleSize) { 
		boolean r = false;
		short offset = (short) -1;
		if(filledBackward()) offset =(short)(freeSpaceOffset - tupleSize);
		else offset = freeSpaceOffset;
		
		if(isValidPut(slotIndex, tupleSize)){
			if(slotIndex >= slotIdx) setSlot(slotIndex, offset, tupleSize);
			else setSlot(slotIndex, getSlotOffset(slotIndex), tupleSize);
			r = true;
		}
		else if (hasDynamicSlots() && getFreeSpace() > (4*(slotIndex-numSlots-1)+tupleSize)){
			Slot s = new Slot(offset, tupleSize);
			growSlots(slotIndex, s);
			r = true;
		}
		return r; 
	}

	
	/**
	 * Uses the next free slot, and returns whether the operation succeeded.
	 * it set the next slot, freeSpaceOffSet, actualSlot
	 */
	@CS316Todo
	@CS416Todo
	
	public int useNextSlot(short tupleSize) { 
		int r = 0;
		short offset = (short) -1;
		if(filledBackward()) offset =(short)(freeSpaceOffset - tupleSize);
		else offset = freeSpaceOffset;
		if(isValidAppend(tupleSize)) {
			setSlot(slotIdx, offset, tupleSize);
			useSpace(tupleSize);
			advanceSlot();
			return 1;
		}
		else if (hasDynamicSlots() && getFreeSpace() > (SLOT_SIZE+tupleSize)){
			Slot s = new Slot(offset, tupleSize);
			growSlots(slotIdx, s);
			r = 1;
		}
		return r;
	}

	/**
	 * Resets a specific slot.
	 */
	@CS316Todo
	@CS416Todo
	public void resetSlot(int slotIndex) {
		Slot s = slots.get(slotIndex);
		//s.length = -1;
		s.offset *= -1; //set to minus 
	}

	/**
	 * Returns a human readable representation of this header;
	 */
	@CS316Todo
	@CS416Todo
	public String toString() { 
		return "tupleSize: " + tupleSize
				+ ", headerSize: " + headerSize
				+ ", freeSpaceOffset: " + freeSpaceOffset
				+ ", bufCapacity: " + bufCapacity
				+ ", totalSlot: " + slots.size()
				+ ", slotIdx: " + slotIdx;
	}

}
