package pf.ls.data;

import java.io.File;
import java.io.IOException;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.NativeRamDirectory;
import org.apache.lucene.store.RAMDirectory;

public class MailIndexReader {
protected Directory directory;
    
    protected IndexReader reader;
    protected int refCount;
    protected String indexDir;
    
    public IndexReader getReader() {
        return reader;
    }
    
    public void open(String indexDir, boolean loadToMem) throws IOException {
    	this.indexDir=indexDir;
    	directory = FSDirectory.open(new File(indexDir), null);
    	
    	if (loadToMem) {
    		directory = new NativeRamDirectory(directory);
    	}
        
        reader = IndexReader.open(directory,false);
        refCount = 1;
    	
    	 /*if (!loadToMem) {
             directory = FSDirectory.getDirectory(indexDir, false);
         } else {
             directory = new NativeRamDirectory(indexDir);
         }

         
         reader = IndexReader.open(directory);
         refCount = 1;*/
    }
    
    public void open(Directory dir, boolean loadToMem) throws IOException {    	
    	directory = dir;
    	
    	if (loadToMem) {
    		directory = new NativeRamDirectory(directory);
    	}
        
        reader = IndexReader.open(directory,false);
        refCount = 1;
    	
    	 /*if (!loadToMem) {
             directory = FSDirectory.getDirectory(indexDir, false);
         } else {
             directory = new NativeRamDirectory(indexDir);
         }

         
         reader = IndexReader.open(directory);
         refCount = 1;*/
    }
    
    public void open(Directory dir, boolean loadToMem, int ref) throws IOException {    	
    	directory = dir;
    	
    	if (loadToMem) {
    		directory = new NativeRamDirectory(directory);
    	}
        
        reader = IndexReader.open(directory,false);
        refCount = ref;
    	
    	 /*if (!loadToMem) {
             directory = FSDirectory.getDirectory(indexDir, false);
         } else {
             directory = new NativeRamDirectory(indexDir);
         }

         
         reader = IndexReader.open(directory);
         refCount = 1;*/
    }
    
    
    public synchronized void close() throws IOException {
        if (--refCount == 0) {
            reader.close();
            directory.close();
        }
    }
    
    public synchronized MailIndexReader replicate() {
        ++refCount;
        return this;
    }

	public void closeWithOutDir() throws IOException {
		if (--refCount == 0) {
            reader.close();
        }

	}

	public String getIndexdir() {
		return indexDir;
	}
    
	public Directory getDirectory() {
		return directory;
	}
}
