package pf.is.tool;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.math.NumberUtils;
import org.apache.log4j.Logger;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.StaleReaderException;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.LockObtainFailedException;

import pf.is.data.EmlMeta;
import pf.is.data.LogMeta;
import pf.is.data.MailMeta;
import pf.is.parser.MailParser;
import pf.utils.DateUtil;
import pf.utils.MailConstants;
import pf.utils.PartationUtil;
import pf.utils.PathUtil;
import pf.utils.XMLUtil;
import pf.utils.data.Path;
import pf.utils.data.Segments;
import pf.utils.index.IndexBuilder;

public class MailDataParseToolTest {
	public static final Logger LOG=Logger.getLogger(MailDataParseToolTest.class.getName());
	public boolean isReadEnd=false;
	int lsNum;
	
	
	public int getLsNum() {
		return lsNum;
	}

	public void setLsNum(int lsNum) {
		this.lsNum = lsNum;
	}

	@SuppressWarnings("unchecked")
	public boolean exec(String basePathStr) throws IOException {
		LOG.info("minor start");
		//create segment
		Path basePath = new Path(basePathStr);
		Path path = basePath.cat(BuildMinorIndexTool.INDEX).cat(BuildMinorIndexTool.MINORINDEX);
		
		PathUtil.mkdirs(path);
		Segments segs = new Segments(path);
		int newseg = segs.createNewSegment();
		Path segPath = path.cat(newseg+"");
		MailParser.init();
		
		BufferedReader stdIn=new BufferedReader(new InputStreamReader(System.in, "utf-8"));
			String line = "";
			//get the number of creating-index thread,the default value is 10
			int threadNum=NumberUtils.toInt(XMLUtil.readValueStrByKey("index_max_thread_num"),10);
			Path indexFolders[]=new Path[threadNum];
			//each folder's IndexWriter,one thread is as to one folder
			IndexBuilder[] indexBuilders=new IndexBuilder[threadNum];
			for(int i=0;i<threadNum;i++) {
				PathUtil.mkdirs(segPath.cat(MailConstants.INDEXFOLDER+i));
				indexFolders[i]=segPath.cat(MailConstants.INDEXFOLDER+i);
				indexBuilders[i]=new IndexBuilder(indexFolders[i]);
				indexBuilders[i].prepare();
			}
			
			Thread threads[]=new Thread[threadNum];
			LinkedBlockingQueue[] queues=new LinkedBlockingQueue[threadNum];
			for(int i=0;i<queues.length;i++) {
				queues[i]=new LinkedBlockingQueue<String>(MailConstants.QUEUE_MAX+10000);
			}
			HashMap[] maps=new HashMap[threadNum];
			for(int i=0;i<maps.length;i++) {
				maps[i]=new HashMap<String,List<String>>();
			}
			IndexReader[] majorReaders=null;
			IndexReader[] minorReaders=null;
			
			class KvDataToIndex implements Runnable {
				 public MailDataParseToolTest mdpt;
				 public LinkedBlockingQueue<String> lines;
				 public IndexBuilder builder;
				 public CountDownLatch latch;
				 public HashMap map;
				 public IndexReader[] majorReaders;
				 public IndexReader[] minorReaders;
				 KvDataToIndex(MailDataParseToolTest mdpt,LinkedBlockingQueue<String> lines,IndexBuilder builder,CountDownLatch latch,HashMap map,IndexReader[] majorReaders,IndexReader[] minorReaders) {
					 this.mdpt=mdpt;
					 this.lines=lines;
					 this.builder=builder;
					 this.latch=latch;
					 this.map=map;
					 this.majorReaders=majorReaders;
					 this.minorReaders=minorReaders;
				 }
				public void run() {
					 LOG.info(Thread.currentThread().getName()+" is Running!");
					 while(lines.size()>0||mdpt.isReadEnd==false) {
				     String getStr=null;
				     try {
						getStr=lines.take();
					 } catch (InterruptedException e1) {
						LOG.error("",e1);
					 }
					 if(getStr!=null&&getStr.equals("GET OVER")) {
					    LOG.info(Thread.currentThread().getName()+" Getted The Over Sign.");
						break;
					  } else{
						 LogMeta log=parseLog(getStr,map,majorReaders,minorReaders,builder);
							if (log == null) {
								continue;
							} 
							long usrid = log.getUsrid();
							int isNo = PartationUtil.getIsId(usrid+"", lsNum);
							
							if (isNo == slice) {
								long parseStart=System.currentTimeMillis();
								EmlMeta eml = MailParser.parseMessage(log.getId(), log.isIspam());
								long parseInterval=System.currentTimeMillis()-parseStart;
								System.out.println("MailParser parse this mail takes "+parseInterval+"ms");
								if (eml == null) {
									continue;
								}
								if((eml.getTxtBody().length()+eml.getHtmlBody().length()) > MailConstants.MAX_MAILCONTENT_LENGTH) {
									LOG.error("This mail with usrid="+usrid+","+"emlkey="+log.getId()+","+"docid="+log.getDocid()+" is too long-content!");
								    continue;
								}
								MailMeta mailMeta = new MailMeta(eml.getSubject(), eml.getTxtBody()+" "+eml.getHtmlBody(), eml.getFrom(), eml.getTo(), log.getId(), log.getTime(), log.getUsrid(), log.getFolder(), eml.getAttaches(),log.getDocid());
								builder.setIndexInfo(mailMeta.getDocid(), mailMeta);
								try {
									long indexStart=System.currentTimeMillis();
									builder.index();
									long indexInterval=System.currentTimeMillis()-indexStart;
									System.out.println("IndexBuilder index this mail takes "+indexInterval+"ms");
								} catch (Exception e) {
									LOG.error("",e);
								}
							} 
					 }
					 
				 }
					if(map.size()>0) {
						deleteIndex(map,majorReaders,minorReaders,builder);
					}
					LOG.info(Thread.currentThread().getName()+" successfully completed!");
					latch.countDown();
				 }
			}
			
			CountDownLatch latches=new CountDownLatch(threadNum);
			
			int minorParseStartH=DateUtil.genCurrentHour();
			Date minorParseStartT=new Date();
			SimpleDateFormat format=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			
			for(int i=0;i<threads.length;i++) {
				threads[i]=new Thread(new KvDataToIndex(this,queues[i],indexBuilders[i],latches,maps[i],majorReaders,minorReaders));
				threads[i].start();
			}
			
			int cursor=0;
			while ((line = stdIn.readLine()) != null) {
				String usridStr=null;
				Matcher matcher=Pattern.compile("\\s+user[_]id[=](.*?)(?:(?:\\s+)|(?=$))").matcher(line);
				if(matcher.find()) {
					usridStr=matcher.group(1);
				}
				long usrid=-1L;
				try {
					usrid=Long.parseLong(usridStr);
				} catch(Exception e) {
					LOG.error("Error line:"+line);
					LOG.error(e);
				}
				try {
					queues[(int)Math.abs(usrid%threadNum)].put(line);
				} catch (InterruptedException e) {
					LOG.error("",e);
				}
	//LOG.info(cursor+" line......................");
				cursor++;
			}
		   isReadEnd=true;
		   for(int i=0;i<queues.length;i++) {
			   try {
				queues[i].put("GET OVER");
			} catch (InterruptedException e) {
				LOG.error("",e);
			}
		  }
		   LOG.info("the readLine-process is over. "+"isReadEnd="+isReadEnd);
        try {
			latches.await();
		} catch (InterruptedException e) {
			LOG.error("", e);
		}
		for(IndexBuilder builder:indexBuilders) {
			builder.finish();
		}
		if(majorReaders!=null) {
		for(IndexReader reader:majorReaders) {
			try {
				reader.flush();
				reader.close();
			} catch (IOException e) {
				LOG.error("",e);
			}
		}
		}
		if(minorReaders!=null) {
		for(IndexReader reader:minorReaders) {
			try {
				reader.flush();
				reader.close();
			} catch (IOException e) {
				LOG.error("",e);
			}
		}
		}
		LOG.info("the index-process is successfully over.");
		int minorParseEndH=DateUtil.genCurrentHour();
		Date minorParseEndT=new Date();
		LOG.info("Start running minor.sh's MailDataParseToolTest step at minorParseStartT="+format.format(minorParseStartT)+" minorParseStartH="+minorParseStartH);
		LOG.info("End running minor.sh's MailDataParseToolTest step at minorParseEndT="+format.format(minorParseEndT)+" minorParseEndH="+minorParseEndH);
		LOG.info("minorParseStartH="+minorParseStartH+" minorParseEndH="+minorParseEndH);
		MailParser.close();
		stdIn.close();
		return true;
	}
	
	@SuppressWarnings("deprecation")
	public static LogMeta parseLog(String line,HashMap<String,List<String>> map,IndexReader[] majorReaders,IndexReader[] minorReaders,IndexBuilder builder) {
//		System.out.println("parsing :"+line);
		LogMeta log = new LogMeta();
		Map<String, String> params = new HashMap<String, String>();
		
		String[] subs = line.split("\\s+");
		for (String sub : subs) {
			String[] kv = sub.split("=");
			if (kv != null && kv.length == 2) {
				params.put(kv[0], kv[1]);
			}
		}
		
		if ((!params.containsKey("action") && !params.containsKey("act")) || !params.containsKey("stat")) {
			return null;
		}
		
		String act = params.get("action");
		if (act == null) {
			act = params.get("act");
		}
		String status = params.get("stat");
		if(status.indexOf("success")>=0){
			if(act.equals("save_inbox")|| act.equals("save_spam") || act.equals("save_outbox_new")
					|| act.equals("save_outbox_delete_draft") || act.equals("create_acount")|| act.equals("save_num_quota")
					|| act.equals("save_size_quota")|| act.equals("save_system")|| act.equals("save_single_instance")) {
				//5代表的是草稿箱，3代表的是垃圾邮件箱
				if (params.get("dir_id")!=null&&params.get("dir_id").equals("5")) {
					return null;
				}
				String usridStr = params.get("user_id");
				
				String dirid = params.get("dir_id");
				String msgid = params.get("msg_id");
				
				if (dirid == null) {
					LOG.error("dir_id is null with"+" user_id="+usridStr+" kv_key="+params.get("kv_key"));
				    return null;
				}
				String id = params.get("kv_key");
				if(id==null||id.equals("")) {
					LOG.error("Error log line:"+line);
					return null;
				}
				long usrid = Long.parseLong(usridStr);
				String docId = params.get("email_id");
				long date=Long.parseLong(params.get("display_time"));
				log.setFolder(dirid);
				log.setId(Long.parseLong(id));
				log.setTime(date);
				log.setUsrid(usrid);
				log.setMsgid(msgid);
				if(act.equals("save_spam")) {
				   log.setIspam(true);
				} else {
				   log.setIspam(false);
				}
				log.setDocid(docId);
				LOG.info("[Success:save_mail] user_id="+usrid+" kv_key="+id+" email_id="+docId+" dir_id="+dirid);
				return log;
			} else if(act.equals("delete_email")) {
				//LOG.info("Delete Mail Line: "+line);
				String usridStr = params.get("user_id");
				String usrid = usridStr;
				String mskey=params.get("email_id");
				if(map.containsKey(usrid)) {
	          		 List<String> mSkeys=map.get(usrid);
	          		 if(mSkeys==null) {
	          			List<String> newMSkeys=new ArrayList<String>();
	          			newMSkeys.add(mskey);
	          			map.put(usrid, newMSkeys);
	          		 } else {
	          			map.get(usrid).add(mskey);
	          		 }
	            } else {
	          		  List<String> newMSkeys=new ArrayList<String>();
	      			  newMSkeys.add(mskey);
	      			  map.put(usrid, newMSkeys);
	            }
				if(map.size()==MailConstants.DELETE_INDEX_CACHE) {
					deleteIndex(map,majorReaders,minorReaders,builder);
				}
				return null;
			} else {
				LOG.info("[Error:unkown format] "+line);
				return null;
			}
		}
		return null;
	}
	
	public static void deleteIndex(Map<String,List<String>> mails,IndexReader[] majorReaders,IndexReader[] minorReaders,IndexBuilder builder) {
		LOG.info("start deleting index according to minor-log");
		for(Map.Entry<String, List<String>> entry:mails.entrySet()) {
			String usrid=entry.getKey();
			for(String mskey:entry.getValue()) {
			try {
				builder.indexWriter.deleteDocuments(new Term(IndexBuilder.TAG_INDEX_DOC_ID,mskey));
			} catch (CorruptIndexException e) {
				LOG.error("",e);
			} catch (IOException e) {
				LOG.error("",e);
			}
			}
		}
		
		int partNo = Integer.parseInt(XMLUtil.readValueStrByKey("partNo"));
		int threadNo=Integer.parseInt(XMLUtil.readValueStrByKey("index_max_thread_num"));
		int countMajor=0;
    	int countMinor=0;
    	if(majorReaders!=null) {
		try {
			for(Map.Entry<String, List<String>> entry:mails.entrySet()) {
			  String usrid=entry.getKey();
			  int part=((int)(Math.abs(Long.parseLong(usrid)%threadNo)))%partNo;
			  IndexReader majorReader=majorReaders[part];
			  for(String mskey:entry.getValue()) {
			    countMajor+=majorReader.deleteDocuments(new Term(IndexBuilder.TAG_INDEX_DOC_ID,mskey));
			   }
			  }
		} catch (StaleReaderException e1) {
			LOG.error("",e1);
		} catch (CorruptIndexException e1) {
			LOG.error("",e1);
		} catch (LockObtainFailedException e1) {
			LOG.error("",e1);
		} catch (IOException e1) {
			LOG.error("",e1);
		}
	  }
    	if(minorReaders!=null) {
		try {
			for(Map.Entry<String, List<String>> entry:mails.entrySet()) {
			 String usrid=entry.getKey();
			 int part=((int)(Math.abs(Long.parseLong(usrid)%threadNo)))%partNo;
			 IndexReader minorReader=minorReaders[part];
			 for(String mskey:entry.getValue()) {
			   countMinor+=minorReader.deleteDocuments(new Term(IndexBuilder.TAG_INDEX_DOC_ID,mskey));
			  }
			 }
		} catch (StaleReaderException e) {
			LOG.error("",e);
		} catch (CorruptIndexException e) {
			LOG.error("",e);
		} catch (LockObtainFailedException e) {
			LOG.error("",e);
		} catch (IOException e) {
			LOG.error("",e);
		}
      }
		mails.clear();
		LOG.info("Delete Result: countMajor="+countMajor+" countMinor="+countMinor);
	}
	
	public int slice;
	
	public static void main(String[] args) {
		MailDataParseToolTest tool = new MailDataParseToolTest();
		
		String basePath=null;
		try {
			basePath = "/dataTest";
		} catch (Exception e1) {
			LOG.info("",e1);
		}
		String sliceStr=null;
		try {
			sliceStr = XMLUtil.readValueStrByKey("slice");
		} catch (Exception e1) {
			LOG.info("",e1);
		}
		String lsNumStr=null;
		try {
			lsNumStr = XMLUtil.readValueStrByKey("isNum");
		} catch (Exception e1) {
			LOG.info("",e1);
		}
		tool.slice = Integer.parseInt(sliceStr);
		tool.setLsNum(Integer.parseInt(lsNumStr));
		
		
		
		try {
			tool.exec(basePath);
//			tool.testInput();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			LOG.info("",e);
		}
		
	}

}
