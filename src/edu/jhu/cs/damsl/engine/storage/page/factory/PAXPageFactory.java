package edu.jhu.cs.damsl.engine.storage.page.factory;

import java.io.FileNotFoundException;

import org.jboss.netty.buffer.ChannelBuffer;

import edu.jhu.cs.damsl.catalog.Schema;
import edu.jhu.cs.damsl.catalog.identifiers.PageId;
import edu.jhu.cs.damsl.engine.storage.page.factory.PageFactory;
import edu.jhu.cs.damsl.engine.storage.page.PAXPage;
import edu.jhu.cs.damsl.engine.storage.page.PAXPageHeader;
import edu.jhu.cs.damsl.utils.hw1.HW1.*;

@CS416Todo
public class PAXPageFactory implements PageFactory<PAXPageHeader, PAXPage>
{
  public PAXPage getPage(Integer id, ChannelBuffer buf, Schema sch, byte flags) {
    return new PAXPage(id, buf, sch, flags);
  }

  public PAXPage getPage(PageId id, ChannelBuffer buf, Schema sch, byte flags) {
    return new PAXPage(id, buf, sch, flags);
  }

  public PAXPage getPage(Integer id, ChannelBuffer buf, Schema sch) {
    return new PAXPage(id, buf, sch, (byte)0X00);
  }
  
  public PAXPage getPage(PageId id, ChannelBuffer buf, Schema sch) {
    return new PAXPage(id, buf, sch, (byte)0X00);
  }

  
  /**
   * does not support variable length
   */
  public PAXPage getPage(Integer id, ChannelBuffer buf, byte flags) {
    throw new UnsupportedOperationException("must provide schema,only support fix length");
  }
  /**
   * does not support variable length
   */
  public PAXPage getPage(PageId id, ChannelBuffer buf, byte flags) {
  	throw new UnsupportedOperationException("must provide schema,only support fix length");
  }
  /**
   * does not support variable length
   */
  public PAXPage getPage(Integer id, ChannelBuffer buf) {
  	throw new UnsupportedOperationException("must provide schema,only support fix length");
  }
  /**
   * does not support variable length
   */
  public PAXPage getPage(PageId id, ChannelBuffer buf) {
  	throw new UnsupportedOperationException("must provide schema,only support fix length");
  }

}