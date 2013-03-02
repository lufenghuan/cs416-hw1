package edu.jhu.cs.damsl.utils.hw1;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;

import edu.jhu.cs.damsl.catalog.Catalog;
import edu.jhu.cs.damsl.catalog.Defaults;
import edu.jhu.cs.damsl.catalog.Schema;
import edu.jhu.cs.damsl.catalog.identifiers.FileId;
import edu.jhu.cs.damsl.catalog.identifiers.PageId;
import edu.jhu.cs.damsl.catalog.identifiers.TableId;
import edu.jhu.cs.damsl.catalog.specs.TableSpec;
import edu.jhu.cs.damsl.engine.dbms.DbEngine;
import edu.jhu.cs.damsl.engine.storage.file.ContiguousHeapFile;
import edu.jhu.cs.damsl.engine.storage.file.SlottedHeapFile;
import edu.jhu.cs.damsl.engine.storage.file.factory.ContiguousStorageFileFactory;
import edu.jhu.cs.damsl.engine.storage.file.factory.SlottedStorageFileFactory;
import edu.jhu.cs.damsl.engine.storage.iterator.page.StorageIterator;
import edu.jhu.cs.damsl.engine.storage.page.ContiguousPage;
import edu.jhu.cs.damsl.engine.storage.page.Page;
import edu.jhu.cs.damsl.engine.storage.page.PageHeader;
import edu.jhu.cs.damsl.engine.storage.page.SlottedPage;
import edu.jhu.cs.damsl.engine.storage.page.SlottedPageHeader;
import edu.jhu.cs.damsl.engine.storage.Tuple;
import edu.jhu.cs.damsl.language.core.types.*;
import edu.jhu.cs.damsl.utils.CSVLoader;
import edu.jhu.cs.damsl.utils.Lineitem;
import edu.jhu.cs.damsl.utils.WorkloadGenerator;

public class HW1Terminal extends Thread {
  String prompt;

//  DbEngine<SlottedPageHeader, SlottedPage, SlottedHeapFile> dbms;
//  WorkloadGenerator<SlottedPageHeader, SlottedPage, SlottedHeapFile> generator;
//  
  //======
  //contiguous
  //======
  DbEngine<PageHeader, ContiguousPage, ContiguousHeapFile> dbms;
  WorkloadGenerator<PageHeader, ContiguousPage, ContiguousHeapFile> generator;
  
  
  
  public HW1Terminal(String prompt) {
    this.prompt = prompt;
    
    //SlottedStorageFileFactory f = new SlottedStorageFileFactory();
    
    ContiguousStorageFileFactory f = new ContiguousStorageFileFactory();
    
    dbms = new DbEngine<PageHeader, ContiguousPage, ContiguousHeapFile>(f);

//    generator = 
//      new WorkloadGenerator<SlottedPageHeader, SlottedPage, SlottedHeapFile>(dbms);
  }
  
  public static LinkedList<String> parseCommand(String msg) {
    StringTokenizer tokenizer = new StringTokenizer(msg);
    LinkedList<String> fields = new LinkedList<String>();
    while (tokenizer.hasMoreTokens()) fields.add(tokenizer.nextToken());
    return fields;
  }

  // Helper functions for command line interface.
  public void printHelp() {
    String[] cmds = new String[] {
      "Available commands:",
      "csv <relation name> <file name>",
      "load <catalog file name>",
      "save <catalog file name>",
      "show",
      "page <relation name> <page id>",
      "benchmark <benchmark mode> <relation name> [probability]",
      "full_4k_test",
      "bye"
    };
    String help = ""; for (String c : cmds) { help += c+"\n"; }
    System.out.println(help);
  }

  Page getPageFromFile(TableId tId, int fileIdx, int pageNum) {
    Page r = null;
    List<FileId> files = tId.getFiles();
    if ( !( files == null || files.isEmpty() || fileIdx >= files.size() ) ) {
      FileId fid = files.get(fileIdx);
      if ( pageNum < fid.numPages() ) {
        PageId pid = new PageId(fid, pageNum);
        r = dbms.getStorageEngine().getPage(null, pid, Page.Permissions.READ);
      } else {
        System.err.println("Invalid page number " + Integer.toString(pageNum));
      }
    } else if ( files != null && fileIdx >= files.size() ) {
      System.err.println("Invalid file index " + fileIdx);
    } else {
      System.err.println("Empty relation " + tId.getName());
    }
    return r;
  }

  void printPage(TableSpec ts, Page p) throws TypeException {
    StorageIterator it = p.iterator();
    while ( it.hasNext() ) {
      List<Object> fields = it.next().interpretTuple(ts.getSchema());
      String r = "";
      for (Object o : fields) { r += o.toString()+"|"; }
      System.out.println("Tuple : "+r);            
    }
  }
//  fts.put("orderkey", new IntType());
//  fts.put("partkey", new IntType());
//  fts.put("suppkey", new IntType());
//  fts.put("linenumber", new IntType());
//
//  fts.put("quantity", new DoubleType());
//  fts.put("extendedprice", new DoubleType());
//  fts.put("discount", new DoubleType());
//
//  fts.put("shipdate", new IntType());
//  fts.put("commitdate", new IntType());
//  fts.put("receiptdate", new IntType());
  void loadLineitemCsv(String tableName, String fileName) {
    Schema lineitemSchema = Lineitem.getSchema(tableName);
    TableId tId = null;
    
    // Create the table if necessary.
    if ( !dbms.hasRelation(tableName) ) {
      tId = dbms.addRelation(tableName, lineitemSchema);
      
      System.out.println(
        "Created table " + tableName +
        " with tuple size " + lineitemSchema.getTupleSize());
    } else {
      tId = dbms.getRelation(tableName);
    }
    
    System.out.println("Loading data from file " + fileName + ".");
    System.out.println("Starting timing load data");
    long startTime = System.nanoTime();
    
    CSVLoader csv = new CSVLoader(fileName);
    csv.load(dbms, tId, lineitemSchema);
    dbms.getStorageEngine().getBufferPool().flushPages();
    
    long timeSpan = System.nanoTime() - startTime;
    System.out.println("Loading: "+fileName+" takes" + Long.toString(timeSpan/1000000) + " ms.");

  }

  // Returns whether we're done processing.
  boolean processLine(String cmdStr) {
    LinkedList<String> args = parseCommand(cmdStr);
    if (args.size() < 1) return false;
    String cmd = args.pop();
    
    if (cmd.toLowerCase().equals("bye")) return true;
    else if (cmd.toLowerCase().equals("help")) {

      printHelp();      

    } else if (cmd.toLowerCase().equals("csv")) {
      
      String tableName = args.pop();
      String fileName = args.pop();

      loadLineitemCsv(tableName, fileName);

    } else if (cmd.toLowerCase().equals("load")) {

      String catalogFile = args.pop();
      
      // Reinitialize the DBMS from a catalog file.
//      SlottedStorageFileFactory f = new SlottedStorageFileFactory();      
//      dbms = new DbEngine<SlottedPageHeader, SlottedPage, SlottedHeapFile>(catalogFile, f);
      
      ContiguousStorageFileFactory f = new ContiguousStorageFileFactory();      
      dbms = new DbEngine<PageHeader, ContiguousPage, ContiguousHeapFile>(catalogFile, f);

    } else if (cmd.toLowerCase().equals("save")) {

      String catalogFile = args.pop();
      dbms.saveDatabase(catalogFile);

    } else if (cmd.toLowerCase().equals("show")) {

      System.out.println(dbms.toString());

    } else if (cmd.toLowerCase().equals("page")) {
      
      String relName = args.pop();
      TableSpec ts = null;
      try {
        ts = dbms.getStorageEngine().getCatalog().getTableByName(relName);
      } catch (NullPointerException e) {
        System.err.println("Invalid table name "+relName);
        return false;
      }

      String pageNumStr = args.pop();
      Integer pageNum = -1;
      try {
        pageNum = Integer.valueOf(pageNumStr);
      } catch (NumberFormatException e) {
        System.err.println("Invalid page number "+pageNumStr);
        return false;
      }

      Page p = getPageFromFile(ts.getId(), pageNum/5120, pageNum%5120);
      if ( p != null ) {
        try {
          printPage(ts, p);
        } catch (TypeException e) {
          System.err.println("Invalid tuple type");
          return false;
        }
      }

    } //end page
    else if(cmd.toLowerCase().equals("full_4k_test")){
    	test_full_4k(-1,-1,"",-1.0);
    }
    /*-------------------------------
     * Benchmark  
     *------------------------------*/
    else if (cmd.toLowerCase().equals("benchmark")) {
//    	 generator = 
//    		      new WorkloadGenerator<SlottedPageHeader, SlottedPage, SlottedHeapFile>(dbms);
    	
    	 generator = 
 		      new WorkloadGenerator<PageHeader, ContiguousPage, ContiguousHeapFile>(dbms);
    	 
    	 
      String benchmarkMode = args.pop();
      String tableName = args.pop();

      TableId tid = null;

      try {
          tid = dbms.getStorageEngine().getCatalog().getTableByName(tableName).getId();
      } catch (NullPointerException e) {
          System.err.println("Invalid Table Name.");
          return false;
      }

      System.out.println("Benchmarking in mode " + benchmarkMode + ".");

      try {
        boolean valid = false;
        int requests = Integer.parseInt(args.pop());
        int blocksize = Integer.parseInt(args.pop());
//        int requests = 100;
//        int blocksize = 20;
        
        if (benchmarkMode.toLowerCase().equals("sequential")) {
            
            generator.generate(tid, requests, blocksize,
                               WorkloadGenerator.Workload.Sequential, 0.0);
            valid = true;
        
        } else {
          valid = true;
          
          double p = 0.75;
          if ( args.size() > 0 ) { p = Double.parseDouble(args.pop()); }

          if (benchmarkMode.toLowerCase().equals("almost-sequential")) {
        
            generator.generate(tid, requests, blocksize,
                               WorkloadGenerator.Workload.MostlySequential, p);
        
          } else if (benchmarkMode.toLowerCase().equals("half-half")) {
          
              generator.generate(tid, requests, blocksize,
                                 WorkloadGenerator.Workload.HalfHalf, p);
          
          } else if (benchmarkMode.toLowerCase().equals("almost-random")) {
          
              generator.generate(tid, requests, blocksize,
                                 WorkloadGenerator.Workload.MostlyRandom, p);
          } else {
            valid = false;
          }
        }
        
        if ( !valid ) {
            System.out.println("Incorrect benchmark mode.");
        }
      } catch (NumberFormatException e) {
        System.err.println("Invalid integer argument for benchmark mode");
        return false;
      }
    }

    return false;
  }
  
  public void test_full_4k(int request, int block, String tabName,double prob){
  //default request size and block size
  	int req = 330000;
  	int blo = 1000;
  	double p = 0.75;
  	String tableName = "full_4k";
  	
  	if(request != -1) req = request;
  	if(block != -1) blo = block;
  	if(tabName != "") tableName = tabName; 
  	if(prob !=-1.0) p = prob;
  	
  	// Reinitialize the DBMS from a catalog file.
//  	String catalogFile = "./full_4k";
//    SlottedStorageFileFactory f = new SlottedStorageFileFactory();      
//    dbms = new DbEngine<SlottedPageHeader, SlottedPage, SlottedHeapFile>(catalogFile, f);
//    
//    //workload generator
//    TableId tid = dbms.getStorageEngine().getCatalog().getTableByName(tableName).getId();
//    generator = 
//	      new WorkloadGenerator<SlottedPageHeader, SlottedPage, SlottedHeapFile>(dbms);
  	
  	
  	String catalogFile = "./full_4k";
    ContiguousStorageFileFactory f = new ContiguousStorageFileFactory();      
    dbms = new DbEngine< PageHeader, ContiguousPage, ContiguousHeapFile>(catalogFile, f);
    
    //workload generator
    TableId tid = dbms.getStorageEngine().getCatalog().getTableByName(tableName).getId();
    generator = 
	      new WorkloadGenerator< PageHeader, ContiguousPage, ContiguousHeapFile>(dbms);
    
    
    
    //sequential benchmark
    System.out.println("Warming up ====================================");
    generator.generate(tid, req, blo,WorkloadGenerator.Workload.Sequential, 0.0);
    dbms.getStorageEngine().getBufferPool().clearPool();
    //System.out.println(dbms.toString());
    System.out.println("end of warm up \n \n");
    
    //sequential benchmark
    System.out.println("Sequential benchmark ====================================");
    generator.generate(tid, req, blo,WorkloadGenerator.Workload.Sequential, 0.0);
    dbms.getStorageEngine().getBufferPool().clearPool();
    //System.out.println(dbms.toString());
    System.out.println("end sequential benchmar \n \n ");
    
    //almost sequential
    System.out.println("almost-sequential benchmark ====================================");
    generator.generate(tid, req, blo,WorkloadGenerator.Workload.MostlySequential, p);
    dbms.getStorageEngine().getBufferPool().clearPool();
    System.out.println("almost-sequential \n \n");
    
    //almost random
    System.out.println("almost-random benchmark ====================================");
    generator.generate(tid, req, blo,WorkloadGenerator.Workload.MostlyRandom, p);
    dbms.getStorageEngine().getBufferPool().clearPool();
    System.out.println("almost-random  benchmark \n \n");
    
    //half half
    System.out.println("half-half benchmark ====================================");
    generator.generate(tid, req, blo,WorkloadGenerator.Workload.HalfHalf, p);
    dbms.getStorageEngine().getBufferPool().clearPool();
    System.out.println("half-half benchmark \n \n");

   
    

    
  }

  public void run() {
    BufferedReader in = new BufferedReader(new InputStreamReader(System.in)); 
    for (;;) {
      System.out.print(prompt+" ");
      try {
        String line = in.readLine();
        if (line == null) break;
        if ( processLine(line) ) break;
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }

  public static void main(String[] args) throws Exception {
      HW1Terminal terminal = new HW1Terminal("jasper>>>");
      terminal.start();
  }
}
