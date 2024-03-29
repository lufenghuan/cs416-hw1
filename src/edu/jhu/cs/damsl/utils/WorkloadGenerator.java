package edu.jhu.cs.damsl.utils;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.jhu.cs.damsl.catalog.Defaults;
import edu.jhu.cs.damsl.catalog.identifiers.FileId;
import edu.jhu.cs.damsl.catalog.identifiers.PageId;
import edu.jhu.cs.damsl.catalog.identifiers.TableId;
import edu.jhu.cs.damsl.engine.dbms.DbEngine;
import edu.jhu.cs.damsl.engine.storage.StorageEngine;
import edu.jhu.cs.damsl.engine.storage.file.StorageFile;
import edu.jhu.cs.damsl.engine.storage.page.Page;
import edu.jhu.cs.damsl.engine.storage.page.PageHeader;
import edu.jhu.cs.damsl.engine.storage.page.SlottedPage;


public class WorkloadGenerator<HeaderType extends PageHeader,
                               PageType extends Page<HeaderType>,
                               FileType extends StorageFile<HeaderType, PageType>>
{
  protected static final Logger logger = LoggerFactory.getLogger(WorkloadGenerator.class);
  
  protected  final long FILE_SIZE = Defaults.getDefaultFileSize();
  protected final  int PAGE_SIZE =  Defaults.getSizeAsInteger(Defaults.defaultPageSize,Defaults.defaultPageUnit);
  protected final int PAGE_PER_FILE = (int) (FILE_SIZE/PAGE_SIZE);

  DbEngine<HeaderType, PageType, FileType> dbms;
  StorageEngine<HeaderType, PageType, FileType> storage;

  public enum Workload { Sequential, HalfHalf, MostlySequential, MostlyRandom }

  public WorkloadGenerator(DbEngine<HeaderType, PageType, FileType> _dbms) {
    this.dbms = _dbms;
    this.storage = dbms.getStorageEngine();
  }

  // Issues a block of read requests to the storage engine.
  void request(FileId f, Random random, Workload mode, double p, int max, int offset, int count) {
    boolean sequential =
      (mode == Workload.HalfHalf?           random.nextDouble() >= 0.5 :
        (mode == Workload.MostlySequential? random.nextDouble() <  p :
          (mode == Workload.MostlyRandom?   random.nextDouble() >= p : true)));

    for (int i = 0; i < count; ++i) {
      int pageNum = sequential? ((offset+i) % max) : random.nextInt(max);
      PageId pid = new PageId(f, pageNum);
      storage.getPage(null, pid, Page.Permissions.READ);
    }
  }
  
  void request(List<FileId> fs, Random random, Workload mode, double p, int max, int offset, int count) {
	    boolean sequential =
	    		(mode == Workload.HalfHalf?           random.nextDouble() >= 0.5 :
	    			(mode == Workload.MostlySequential? random.nextDouble() <  p :
	    				(mode == Workload.MostlyRandom?   random.nextDouble() >= p : true)));

	    int last = offset;
	    for (int i = 0; i < count; ++i) {
	    	//System.out.println(i);
	    	int pageNum = sequential? (last+1) : random.nextInt(max);
	    	last = pageNum;
	    	FileId f = fs.get(pageNum%max/PAGE_PER_FILE);
	    	PageId pid = new PageId(f, pageNum%max%PAGE_PER_FILE);
	    	try{
	    		storage.getPage(null, pid, Page.Permissions.READ);
	    	} catch(Exception e){
	    		logger.error("file no:{}, page no:{}",pageNum/PAGE_PER_FILE,pageNum%PAGE_PER_FILE);
	    		}
	    }

  }


  public void generate(TableId t, int requests, int blocksize, Workload mode, double p)
  {
    logger.info(
      "Generating {} requests with block size {} and randomness {}",
      new Object[] { requests, blocksize, p });

    // Get a handle to the first database file supporting the relation.
    LinkedList<FileType> tFiles = storage.getFileManager().getFiles(t);
    if ( tFiles == null || tFiles.isEmpty() ) {
      logger.error("No database files found for relation {}", t.getAddressString());
      return;
    }

    FileType tFile = tFiles.peek();
    FileId fileId = tFile.fileId();
    int numPages = tFile.numPages();

    if ( (mode == Workload.MostlySequential || mode == Workload.MostlyRandom) && p < 0.5 ) { 
      logger.error("Invalid probability for workload, must be greater than 0.5");
      return;
    }

    Random random = new Random();
    int rounds = requests / blocksize;
    int remainder = requests % blocksize;
    
    int totalPages = 0;
    ArrayList<FileId> fileIds = new ArrayList<>(t.getFiles());
    for(FileId fi:fileIds){
    	totalPages+=fi.numPages();
    }
    // Execute rounds.
    System.out.println("Total page number: "+totalPages);
    System.out.println("Starting benchmark");
    long startTime = System.nanoTime();
    
    for (int i = 0; i < rounds; ++i) {
    	//System.out.println("Round:"+i);
      //request(fileId, random, mode, p, numPages, i*blocksize, blocksize);
    	request(fileIds, random, mode, p, totalPages, i*blocksize, blocksize);

    }
    
    // Execute remainder.
    request(fileId, random, mode, p, numPages, rounds*blocksize, remainder);
    
    long timeSpan = System.nanoTime() - startTime;
    System.out.println("Elapsed: " + Long.toString(timeSpan/1000000) + " ms.");
  }
}
