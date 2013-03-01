package edu.jhu.cs.damsl.engine.storage.page;

import java.util.BitSet;
import java.util.List;

import org.jboss.netty.buffer.ChannelBuffer;

import edu.jhu.cs.damsl.catalog.Schema;
import edu.jhu.cs.damsl.language.core.types.Type;
import edu.jhu.cs.damsl.utils.hw1.HW1.*;

@CS416Todo
/**
 * Only implement fixed length tuple
 * @author lufenghuan
 *
 */
public class PAXPageHeader extends PageHeader {

  protected short recordNum;
  protected short recordCapacity;
  protected short attributeNum;
  protected short[] attributeLen;
  protected BitSet exist;
  
  public PAXPageHeader(Schema sch, ChannelBuffer buf, byte flags) {
  	super(sch, buf, flags);
  	assert(sch != null && sch.getTupleSize() > 0);
  	List<Type> types = sch.getTypes();
  	recordNum = (short) types.size();
  	attributeLen = new short [recordNum];
  	for(int i = 0; i < recordNum; i++){
  		attributeLen[i] = types.get(i).getSize().shortValue();
  	}
  	//recordCapacity = buf.capacity()-/()
  	resetHeader();
  }
  
  @Override
  public void writeHeader(ChannelBuffer buf){
	  super.writeHeader(buf);
	  buf.writeShort(recordNum);
	  buf.writeShort(attributeNum);
	  for(short s : attributeLen){
		  buf.writeShort(s);
	  }
	  buf.writeBytes(exist.toByteArray());
  }
  @Override
  public void resetHeader(){
	  
  }
  
  @Override
  public short getHeaderSize(){
	  return 0;
  }
  
}