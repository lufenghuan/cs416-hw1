package edu.jhu.cs.damsl.engine.storage.page.factory;

import java.io.DataInput;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.BitSet;

import edu.jhu.cs.damsl.catalog.Schema;
import org.jboss.netty.buffer.ChannelBuffer;
import edu.jhu.cs.damsl.engine.storage.page.PageHeader;
import edu.jhu.cs.damsl.engine.storage.page.PAXPageHeader;
import edu.jhu.cs.damsl.engine.storage.page.SlottedPageHeader;
import edu.jhu.cs.damsl.engine.storage.page.factory.HeaderFactory;
import edu.jhu.cs.damsl.engine.storage.page.factory.PageHeaderFactory;
import edu.jhu.cs.damsl.utils.hw1.HW1.*;

@CS416Todo
public class PAXPageHeaderFactory
				implements HeaderFactory<PAXPageHeader>
{

	PageHeaderFactory pageHeaderFactory;
  public PAXPageHeaderFactory() {
  	pageHeaderFactory = new PageHeaderFactory();
  }
  
  public PAXPageHeader getHeader(Schema sch, ChannelBuffer buf, byte flags) {
  	return new PAXPageHeader(sch, buf, flags);
  }

  
  // Read the header from the backing buffer into the in-memory header.
  public PAXPageHeader readHeader(ChannelBuffer buf) {
//  	super.writeHeader(buf);
//		buf.writeShort(recordCapacity);
//		buf.writeShort(numRecord);
//		buf.writeShort(attributeNum);
//		for(short s : attributeLen){
//			buf.writeShort(s);
//		}
//		for(short s : offsets){
//			buf.writeShort(s);
//		}
//		buf.writeBytes(exist.toByteArray());
  	PageHeader h = pageHeaderFactory.readHeader(buf);


  	short recordCapacity = buf.readShort();
  	short numRecord = buf.readShort();
  	short attributeNum = buf.readShort() ;
  	short[] attributeLen = new short[attributeNum];
  	short[] offsets= new short [attributeNum];
  	byte [] bits = new byte [(int) Math.ceil(recordCapacity/8)];
  	
  	for(int i=0; i<attributeNum; i++){attributeLen[i]=buf.readShort();}
  	for(int i=0; i<attributeNum; i++){offsets[i]=buf.readShort();}
  	buf.readBytes(bits);

  	PAXPageHeader r = new PAXPageHeader(h, recordCapacity, numRecord,
  			attributeNum,attributeLen,offsets,bits);
    return r;
  }
  
  public PAXPageHeader readHeaderDirect(DataInput f)
      throws IOException
  {
    return null;
  }
}