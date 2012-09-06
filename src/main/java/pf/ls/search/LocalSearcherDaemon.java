package pf.ls.search;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.wltea.analyzer.lucene.IKAnalyzer;

import pf.is.crawl.MinaLogMaster;
import pf.ls.data.MailIndexReader;
import pf.mina.MinaServiceSlave;
import pf.mina.IndexSearchHandlerAdapters.LocalIndexDelete;
import pf.mina.IndexSearchHandlerAdapters.LocalIndexServer;
import pf.mina.handler.DeleteIHandler;
import pf.mina.handler.IHandler;
import pf.utils.DateUtil;
import pf.utils.MailConstants;
import pf.utils.PathUtil;
import pf.utils.XMLUtil;
import pf.utils.data.Path;
import pf.utils.data.Segments;
import pf.utils.index.IndexBuilder;

public class LocalSearcherDaemon {
	public static final Logger LOG=Logger.getLogger(LocalSearcherDaemon.class.getName());

	private static final long CHECK_INTERVAL = 3*1000L;

	protected MinaServiceSlave slave;
	protected MailLocalIndexSearcher searcher;
	protected LocalIndexServer handler = new LocalIndexServer();
	protected LocalIndexDelete deleteHandler = new LocalIndexDelete();
    private Path curMajorPath;
    private Path curMinorPath;
    private int localMajorVer = -1;
    private int localMinorVer = -1;
    private int partNo = -1;
    private int slice = -1;
    private int lsNum;
    private boolean loadToMem;
    
    private MinaLogMaster logMaster;
    private List<Directory> realTimeIndex;
    private List<Directory> oldTimeIndex;
    
    private boolean hasRefresh=false;
    
    public void init(int partNo, String majorPathStr, String minorPathStr, int slice, String[] serverHosts, boolean loadToMem) throws UnknownHostException, IOException {
    	searcher = new MailLocalIndexSearcher(slice, partNo, lsNum);
    	LOG.info(partNo+"-"+majorPathStr+"-"+minorPathStr+"-"+slice+"-"+serverHosts+"-"+loadToMem);
    	LOG.info("start init");
    	logMaster = new MinaLogMaster(Integer.parseInt(XMLUtil.readValueStrByKey("ls_log_master_port")), 8, 150,slice);
    	logMaster.setLsNum(lsNum);
    	LOG.info("log master newed");
    	
    	this.curMajorPath = new Path(majorPathStr);
    	this.curMinorPath = new Path(minorPathStr);
    	this.partNo = partNo;
    	this.slice = slice;
    	this.loadToMem = loadToMem;
    	realTimeIndex = new ArrayList<Directory>();
    	oldTimeIndex = new ArrayList<Directory>();
    	
		if (!check()) {
			LOG.info("no index data found.");
			return;
		}
		
		if(searcher.time==-1) {
	       searcher.time=DateUtil.genCurrentHour();
	       searcher.tag=0;
	    }
		
		Calendar nowH=Calendar.getInstance();
		nowH.setTime(new Date());
		long start=nowH.getTimeInMillis();
		nowH.add(Calendar.HOUR_OF_DAY, 1);
		Calendar nextHour=Calendar.getInstance();
		nextHour.set(Calendar.HOUR_OF_DAY, nowH.get(Calendar.HOUR_OF_DAY));
		nextHour.set(Calendar.MINUTE, 0);
		nextHour.set(Calendar.SECOND, 0);
		nextHour.set(Calendar.MILLISECOND,0);
		long next=nextHour.getTimeInMillis();
		long timeInterval=next-start;
		
		ScheduledExecutorService timer=Executors.newSingleThreadScheduledExecutor();
    	timer.scheduleAtFixedRate(new Runnable(){
    		public void run() {
    			int tmp=-1;
    	    	if((tmp=DateUtil.genCurrentHour())>searcher.time) {
    	    	   int lastTag=searcher.tag;
    	    	   searcher.tag=(tmp-searcher.time)%3;
    	    	   if(lastTag!=searcher.tag) {
    	    	   if(searcher.tag==0) {
    	    		   if(searcher.beforeCache.size()!=0) { 
    	    			   LOG.error("the before-cache has not be cleared!");
    	    		   }
    	    	   } else if(searcher.tag==1) {
    	    		   if(searcher.middleCache.size()!=0) {
    	        		   LOG.error("the middle-cache has not be cleared!");
    	        	   } 
    	    	   } else if(searcher.tag==2) {
    	    		   if(searcher.afterCache.size()!=0) {
    	        		   LOG.error("the after-cache has not be cleared!");
    	        	   }
    	    	   }
    	    	  }
    	    	} else if((tmp=DateUtil.genCurrentHour())<searcher.time) {
    	    	   int lastTag=searcher.tag;
    	    	   searcher.tag=(24+tmp-searcher.time)%3;
    	    	   if(lastTag!=searcher.tag) {
    	    	   if(searcher.tag==0) {
    	    		   if(searcher.beforeCache.size()!=0) {  
    	    			   LOG.error("the before-cache has not be cleared!");
    	    		   }
    	    	   } else if(searcher.tag==1) {
    	    		   if(searcher.middleCache.size()!=0) {
    	        		   LOG.error("the middle-cache has not be cleared!");
    	        	   } 
    	    	   } else if(searcher.tag==2) {
    	    		   if(searcher.afterCache.size()!=0) {
    	        		   LOG.error("the after-cache has not be cleared!");
    	        	   }
    	    	   }
    	    	  }
    	    	} else if((tmp=DateUtil.genCurrentHour())==searcher.time) {
    	    	   int lastTag=searcher.tag;
    	    	   searcher.tag=0;
    	    	   if(lastTag!=searcher.tag) {
    	    	   if(searcher.beforeCache.size()!=0) {
    	    		   LOG.error("the before-cache has not be cleared!");
    	    	   }
    	    	  }
    	    	}
    		}
    	}
    	,timeInterval,1*60*60*1000,TimeUnit.MILLISECONDS);
    	
		IHandler handler = getHandler();
		DeleteIHandler deleteHandler = getDeleteHandler();
//		String[] serverHosts = {"172.31.13.105", "172.31.13.196"};
		int[] serverPorts = {Integer.parseInt(XMLUtil.readValueStrByKey("ls_send_ds_result"))};
		int slavePort = Integer.parseInt(XMLUtil.readValueStrByKey("ls_recive_ds_keywords"));
int heartBeatInterval = 10000;//change 30000 to 10000
		LOG.info("start new slave...");
		slave = new MinaServiceSlave(handler, deleteHandler, serverHosts, serverPorts,
                InetAddress.getLocalHost().getHostName(), slavePort,
                getServiceType(), slice, 0, heartBeatInterval, 15);
    }
    
    public void update(int partNo, Path majorPath, Path minorPath) throws IOException {
    	MailIndexReader[] majors = new MailIndexReader[partNo];
    	MailIndexReader[] minors = new MailIndexReader[partNo];
    	
    	boolean clearRealTime = false;;
    	/*int minorcount = 0;
    	if (minorPathes != null && minorPathes.length > 0) {
    		minorcount = partNo * minorPathes.length;
    	}
    	MailIndexReader[] minors = new MailIndexReader[minorcount];*/
    	
    	
    	MailIndexReader[] oldmajors = searcher.getMajorReaders();
    	
    	MailIndexReader[] oldminors = searcher.getMinorReaders();
    	
    	if (majorPath == null) {
    		if(PathUtil.exists(new Path(XMLUtil.readValueStrByKey("base_is_path")).cat("RefreshSearchReader"))) {
    		    if(oldmajors!=null) {
    			  for(MailIndexReader r:oldmajors) {
    		      	  r.getReader().close();
    		      }
    		    }
    			for(int i=0;i<partNo;i++) {
    			   majors[i]=new MailIndexReader();
    			   majors[i].open(oldmajors[i].getIndexdir(), loadToMem);
    		   }
    		   hasRefresh=true;
    		} else {
    		   majors = oldmajors;
    		   for (MailIndexReader major : majors) {
    			major.replicate();
    		   }
    		}
    	} else {
    		clearRealTime = true;
    		for (int i=0; i<partNo; i++) {
        		majors[i] = new MailIndexReader();
        		majors[i].open(majorPath.cat(MailConstants.PART_PRE+i).cat(IndexBuilder.LUCENE_INDEX_DIR).getAbsolutePath(), loadToMem);
    		}
    	}
    	
    	/*if (minorPathes == null) {
    		minors = oldminors;
    		if (minors != null) {
	    		for (MailIndexReader minor : minors) {
	    			minor.replicate();
	    		}
    		}
    	} else {
    		for (Path minorPath : minorPathes) {
	    		for (int i=0; i<partNo; i++) {
	        		minors[i] = new MailIndexReader();
	        		minors[i].open(minorPath.cat(MailConstants.PART_PRE+i).cat(IndexBuilder.LUCENE_INDEX_DIR).getAbsolutePath(), loadToMem);
	        	}
    		}
    	}*/
    	
    	if (minorPath == null) {
    		if(PathUtil.exists(new Path(XMLUtil.readValueStrByKey("base_is_path")).cat("RefreshSearchReader"))) {
    		   if(oldminors!=null) {
    			   for(MailIndexReader r:oldminors) {
    				   r.getReader().close();
    			   }
    		   }
    			for(int i=0;i<partNo;i++) {	
    			  minors[i]=new MailIndexReader();
    			  minors[i].open(oldminors[i].getIndexdir(), loadToMem);
    		    }
    		} else {
    		  minors = oldminors;
    		  if (minors != null) {
	    		for (MailIndexReader minor : minors) {
	    			minor.replicate();
	    		 }
    		   }
    		}
    	} else {
    		clearRealTime = true;
    		for (int i=0; i<partNo; i++) {
        		minors[i] = new MailIndexReader();
        		minors[i].open(minorPath.cat(MailConstants.PART_PRE+i).cat(IndexBuilder.LUCENE_INDEX_DIR).getAbsolutePath(), loadToMem);
    		}
    	}
    	
    	
    	if (clearRealTime) {
    		/*for (Directory dir : oldTimeIndex) {
    			dir.close();
    		}*/
    		oldTimeIndex.clear();
    		
    		oldTimeIndex.addAll(realTimeIndex);
    		realTimeIndex.clear();
    		
    		MailLocalIndexSearcher newSearcher = new MailLocalIndexSearcher(slice, partNo, lsNum);
    		
    		MailIndexReader[] oldtimes = new MailIndexReader[oldTimeIndex.size()];
    		for (int i=0; i< oldTimeIndex.size(); i++) {
    			oldtimes[i] = new MailIndexReader();
    			oldtimes[i].open(oldTimeIndex.get(i), false);
    		}
    		newSearcher.open(majors, minors, oldtimes);
        	MailLocalIndexSearcher oldSearcher = this.searcher;
        	searcher = newSearcher;
        	handler.setSearcher(newSearcher);
        	deleteHandler.setSearcher(newSearcher);
        	logMaster.setSearcher(newSearcher);
            oldSearcher.closeAll();//At last,I find it by working very hard!
    	} else {
    		MailIndexReader[] realtimes = new MailIndexReader[realTimeIndex.size()+oldTimeIndex.size()];
    		for (int i=0; i< realTimeIndex.size(); i++) {
    			realtimes[i] = new MailIndexReader();
    			realtimes[i].open(realTimeIndex.get(i), false);
    		}
    		for (int i= realTimeIndex.size(); i< realtimes.length; i++) {
    			realtimes[i] = new MailIndexReader();
    			realtimes[i].open(oldTimeIndex.get(i-realTimeIndex.size()), false);
    		}
    		MailLocalIndexSearcher newSearcher = new MailLocalIndexSearcher(slice, partNo, lsNum);
    		newSearcher.open(majors, minors, realtimes);
        	MailLocalIndexSearcher oldSearcher = this.searcher;
        	searcher = newSearcher;
        	handler.setSearcher(newSearcher);
        	deleteHandler.setSearcher(newSearcher);
        	logMaster.setSearcher(newSearcher);
            // oldSearcher.closeAll();//change oldSearcher.close() to oldSearcher.closeAll()
            oldSearcher.close();
            /*if(oldSearcher.realTimeReaders!=null) {
             for(MailIndexReader realTimeReader:oldSearcher.realTimeReaders) {
            	 realTimeReader.getReader().close();
             }
            }*/
    	}
    	
    	
    }
    
    public IHandler getHandler() {
    	return handler;
    }
    
    public DeleteIHandler getDeleteHandler() {
    	return deleteHandler;
    }
    
    public int getServiceType() {
    	return 0;
    }
    
    
    public void exec() throws InterruptedException, IOException{
    	while (true) {
    		check();
    		Thread.sleep(CHECK_INTERVAL);
    	}
    }
    
    private boolean check() throws IOException {
    	Path latestMajorPath = null;
    	Path latestMinorPath = null;
    	boolean needupdate = false;
    	int majorVer = getLatestVer(curMajorPath, true);
    	if (majorVer >= 0) {
    		latestMajorPath = curMajorPath.cat(majorVer + "");
    		
    		needupdate = true;
    	} 
    	
    	/*List<Path> minorVer = getLatestMinor();
    	if (minorVer != null && minorVer.size() > 0) {
    		latestMinorPath = minorVer.toArray(new Path[minorVer.size()]);
    		needupdate = true;
    		LOG.info("minor update:"+needupdate);
    	}*/
    	int minorVer = getLatestVer(curMinorPath, false);
    	if (minorVer >= 0) {
    		latestMinorPath = curMinorPath.cat(minorVer + "");
    		
    		needupdate = true;
    	}
    	
    	if (!needupdate) {
	    	List<Directory> rtIndexes = logMaster.removeIndexes();
	    	if (rtIndexes != null && rtIndexes.size() > 0) {
		    	IndexReader[] mergeinputs = new IndexReader[rtIndexes.size()];    	
		    	//TODO: merge rtIndex
		    	for (int i=0; i<rtIndexes.size(); i++) {
		    		mergeinputs[i] = IndexReader.open(rtIndexes.get(i));
		    	}
		    	Directory mergedRtIndex = new RAMDirectory(); 
		    	IndexWriter indexWriter = new IndexWriter(mergedRtIndex, 
						new IKAnalyzer(false), true, IndexWriter.MaxFieldLength.LIMITED);
		    	indexWriter.setMaxMergeDocs(1024);
		        indexWriter.setMergeFactor(100);
				indexWriter.addIndexes(mergeinputs);
				indexWriter.close();
				realTimeIndex.add(mergedRtIndex);
				needupdate = true;
	    	}
    	}
		
    	
    	if (needupdate) {
    		update(partNo, latestMajorPath, latestMinorPath);
    		
    		if(minorVer==0) {
        		Segments newSeg = new Segments(curMinorPath);
        		String[] newTags = {MailConstants.MERGED_TAG};
        		List<Integer> newMajorlist = newSeg.findSegments(newTags, null);
        		int newLastVer = -1;
        		//find last major no
        		for (Integer newSegNo : newMajorlist) {
        			if (newSegNo > newLastVer) {
        				newLastVer = newSegNo;
        			}
        		}
        		if(newLastVer>0&&PathUtil.exists(curMinorPath.cat(newLastVer+"").cat("LASTTAG"))&&newMajorlist.size()==2&&newMajorlist.contains(0)) {
        			  PathUtil.delete(curMinorPath.cat(newLastVer+""));
        		}
        		}
    		
    		if(hasRefresh==true) {
    		  if(PathUtil.exists(new Path(XMLUtil.readValueStrByKey("base_is_path")).cat("RefreshSearchReader"))) {
    			if(searcher.tag==0) {
    				LOG.info("check: tag="+searcher.tag+" time="+searcher.time);
    				if(searcher.middleCache.size()>0) {
    				   LOG.info("clear searcher.middleCache.");
    				   searcher.middleCache.clear();
    				}
    				PathUtil.delete(new Path(XMLUtil.readValueStrByKey("base_is_path")).cat("RefreshSearchReader"));
    			} else if(searcher.tag==1) {
    				LOG.info("check: tag="+searcher.tag+" time="+searcher.time);
    				if(searcher.afterCache.size()>0) {
    				   LOG.info("clear searcher.afterCache.");
    				   searcher.afterCache.clear();
    				}
    				PathUtil.delete(new Path(XMLUtil.readValueStrByKey("base_is_path")).cat("RefreshSearchReader"));
    			} else if(searcher.tag==2) {
    				LOG.info("check: tag="+searcher.tag+" time="+searcher.time);
    				if(searcher.beforeCache.size()>0) {
    				   LOG.info("clear searcher.beforeCache.");
    				   searcher.beforeCache.clear();
    				}
    				PathUtil.delete(new Path(XMLUtil.readValueStrByKey("base_is_path")).cat("RefreshSearchReader"));
    			}
    		}
    		hasRefresh=false;
    	 }
    		return true;
    	}
    	
    	
    	
    	
    	return false;
	}
    
    /*private List<Path> getLatestMinor() throws IOException {
    	Path[] minorDays = curMinorPath.listPathes();
    	String[] tags = {MailConstants.MERGED_TAG};
    	List<Path> result = new ArrayList<Path>();
    	
    	
    	for (Path minorDay : minorDays) {
    		int lastminor = -1;
    		Segments minorSeg = new Segments(minorDay);
    		List<Integer> minorList = minorSeg.findSegments(tags, null);
    		for (Integer minorNo : minorList) {
    			if (minorNo > lastminor) {
    				lastminor = minorNo;
    			}
    		}
    		
    		if (lastminor > localMinorVer) {
    			result.add(minorDay.cat(lastminor+""));
    			localMinorVer = lastminor;
    		}
    	}
		return result;
	}*/

	private int getLatestVer(Path majorPath, boolean isMajor) throws IOException {
		Segments seg = new Segments(majorPath);
		String[] tags = {MailConstants.MERGED_TAG};
		List<Integer> majorlist = seg.findSegments(tags, null);
		int lastVer = -1;
		//find last major no
		for (Integer segNo : majorlist) {
			if (segNo > lastVer) {
				lastVer = segNo;
			}
		}
		
		if(!isMajor) {
			if(lastVer>0&&PathUtil.exists(majorPath.cat(lastVer+"").cat("LASTTAG"))&&majorlist.size()==2&&majorlist.contains(0)) {
				lastVer=0;
			}
		}
		
		if (isMajor) {
			LOG.info("[INFO] action=check_version local_major="+localMajorVer +" last_major="+lastVer);
			if (lastVer > localMajorVer) {
				localMajorVer = lastVer;
				return lastVer;
			} else {
				return -1;
			}
		} else {
			LOG.info("[INFO] action=check_version local_minor="+localMinorVer +" last_minor="+lastVer);
			if (lastVer > localMinorVer||(lastVer==0&&localMinorVer>0)) {
				localMinorVer = lastVer;
				return lastVer;
			} else {
				return -1;
			}
		}
	}

	private class LocalDaemon extends Thread {
        public void run() {
            try {
                exec();
            } catch (Exception e) {
                LOG.error("Exception in local daemon:", e);
            } finally {
                close();
            }
        }
    }
    
    private LocalDaemon localDaemon = new LocalDaemon();

    public void start() {
        localDaemon.setDaemon(true);
        localDaemon.start();
    }

    public void stop() throws Exception {
        localDaemon.interrupt();
    }
    
    private void close() {
        if (slave != null) {
            slave.stop();
        }
    }

	public int getLsNum() {
		return lsNum;
	}

	public void setLsNum(int lsNum) {
		this.lsNum = lsNum;
	}

    public  void setSearcher(MailLocalIndexSearcher searcher) {
    	this.searcher=searcher;
    }
    
    public  MailLocalIndexSearcher getSearcher() {
    	return searcher;
    }
    
}
