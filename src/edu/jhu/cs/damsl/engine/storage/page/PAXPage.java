package edu.jhu.cs.damsl.engine.storage.page;

import org.jboss.netty.buffer.ChannelBuffer;

import edu.jhu.cs.damsl.catalog.Schema;
import edu.jhu.cs.damsl.catalog.identifiers.PageId;
import edu.jhu.cs.damsl.engine.storage.Tuple;
import edu.jhu.cs.damsl.engine.storage.iterator.page.PAXPageterator;
import edu.jhu.cs.damsl.engine.storage.iterator.page.SlottedPageIterator;
import edu.jhu.cs.damsl.engine.storage.iterator.page.StorageIterator;
import edu.jhu.cs.damsl.engine.storage.page.factory.HeaderFactory;
import edu.jhu.cs.damsl.engine.storage.page.factory.PAXPageHeaderFactory;
import edu.jhu.cs.damsl.engine.storage.page.factory.SlottedPageHeaderFactory;
import edu.jhu.cs.damsl.utils.hw1.HW1.*;

@CS416Todo
public class PAXPage extends Page<PAXPageHeader> {
	public static final HeaderFactory<PAXPageHeader> headerFactory 
	= new PAXPageHeaderFactory();

	public PAXPage(Integer id, ChannelBuffer buf, Schema sch, byte flags) {
		this(new PageId(id), buf, sch, flags);
	}

	public PAXPage(PageId id, ChannelBuffer buf, Schema sch, byte flags) {
		super(id, buf, sch, flags);
	}

	// Factory accessors
	@CS416Todo
	public HeaderFactory<PAXPageHeader> getHeaderFactory() {
		return headerFactory;
	}

	// Tuple accessors.

	// The default tuple retrieval method is via iteration.
	@CS416Todo
	public StorageIterator iterator() { return new PAXPageterator(getId(), this); }

	// Append a variable-length tuple to the page.
	@CS416Todo  
	public boolean putTuple(Tuple t, short requestedSize) { 
		if(header.getFreeSpace() >= (requestedSize-Tuple.headerSize)){
			int offset = Tuple.headerSize;
			for(int i = 0; i < header.attributeNum; i++){
				this.setBytes(header.offsets[i]+header.numRecord*header.attributeLen[i],
						t, offset,header.attributeLen[i]);
				offset += header.attributeLen[i];
			}
			header.exist.set(header.numRecord);
			header.useSpace((short) (requestedSize-Tuple.headerSize));
			return true;
		}
		return false; 

	}

	// Append a fixed-size tuple to the page.
	@CS416Todo  
	public boolean putTuple(Tuple t) { 
		return putTuple(t, header.tupleSize);
	}
	
	public Tuple getTuple(short index){
		if(!header.isValidTuple(index))return null;
		Tuple t = Tuple.emptyTuple(header.tupleSize,false);
		int tupleOffset = Tuple.headerSize;
		for(int i = 0; i < header.attributeNum; i++){
//			System.out.println("="+(header.offsets[i]+index*header.attributeLen[i]));
//			if(header.attributeLen[i]==4)
//				System.out.println(this.getInt(header.offsets[i]+index*header.attributeLen[i]));
//			else System.out.println(this.getDouble(header.offsets[i]+index*header.attributeLen[i]));
			t.setBytes(tupleOffset,this, 
					header.offsets[i]+index*header.attributeLen[i],header.attributeLen[i]);
			tupleOffset += header.attributeLen[i];
		}
		t.writerIndex(header.tupleSize+Tuple.headerSize);
		
		return  t;
	}
	
	public boolean removeTuple(int index){
		if(header.isValidTuple(index)) return false;
		header.exist.set(index, false);
		return true;
		
	}

}