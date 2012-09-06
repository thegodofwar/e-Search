package pf.is.tool;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import odis.serialize.lib.MD5Writable;
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

public class LostMailDataParseTool {
	public static final String SEGMENT = "crwalsegment";
	public static final String ADD = "add";
	public static final String DEL = "del";
	public static final int BUFFERSIZE = 1024;
	int lsNum;
	
	
	public int getLsNum() {
		return lsNum;
	}

	public void setLsNum(int lsNum) {
		this.lsNum = lsNum;
	}
    
	/*
	 * @author liufukun
	 * 从最后拷贝log文件成功的下一个小时
	 * 到当前时间的上一个小时开始拷贝lost-log文件
	 */
	public boolean copyLostLogsFromMss() throws Exception {
		Date lastTime_buildMinor=null;
		try {
			lastTime_buildMinor=XMLUtil.readStartTimeBuildMinor();
		} catch (Exception e) {
			e.printStackTrace();
		}
		Calendar start_calendar=Calendar.getInstance();
		start_calendar.setTime(lastTime_buildMinor);
		start_calendar.add(Calendar.HOUR_OF_DAY, 1);
		
		SimpleDateFormat format=new SimpleDateFormat("yyyyMMddHH");
		
		Date end_date=XMLUtil.readEndTimeBuildMinor();
		Calendar end_calendar=Calendar.getInstance();
		end_calendar.setTime(end_date);
		
		//mss日志服务器上存放日志的位置
		String mss_logpath=XMLUtil.readValueStrByKey("mss_logpath");
		//LS本地搜索服务器上保存拷贝日志的位置
		String local_logpath=XMLUtil.readValueStrByKey("local_logpath");
		//mss日志服务器的IP地址集（以逗号分割）
		String mss_ips=XMLUtil.readValueStrByKey("mss_ips");
		String mssips[]=mss_ips.split(",");
		Calendar temp_calendar=start_calendar;
		while(temp_calendar.before(end_calendar)) {
			String tempTime=format.format(temp_calendar.getTime());
			Runtime rt=Runtime.getRuntime();
			for(String mss_ip:mssips) {
				String cmd="scp "+mss_ip+":"+mss_logpath+"/"+tempTime+XMLUtil.readValueStrByKey("log_suffix")+" "+local_logpath;
				System.out.println("exec cmd:"+cmd);
				try {
					Process p = rt.exec(cmd);
					int rc = p.waitFor();
					System.out.println("exec return:"+rc);
				} catch (IOException e) {
					e.printStackTrace();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			temp_calendar.add(Calendar.HOUR_OF_DAY, 1);
		}
		System.out.println("copy finally success");
		return true;
	}
	
	public boolean exec() throws IOException {
		System.out.println("Lost start");
		//create segment
		String basePathStr=null;
		try {
			basePathStr = XMLUtil.readValueStrByKey("base_is_path");
		} catch (Exception e) {
			e.printStackTrace();
		}
		Path basePath = new Path(basePathStr);
		String day = DateUtil.genCurrentDayStr();
		Path dayPath = basePath.cat(SEGMENT).cat(ADD).cat(day);
		
		PathUtil.mkdirs(dayPath);
		Segments segs = new Segments(dayPath);
		int newseg = segs.createNewSegment();
		Path segPath = dayPath.cat(newseg+"");
		MailParser.init();
		
		
		/*Path logPath = new Path(logStr);
		Path[] logs = logPath.listPathes();
		List<Path> finish = new ArrayList<Path>();
//		List<MailMeta> metaBuffer = new ArrayList<MailMeta>();
		for (Path msslog : logs) {
			File logFile = msslog.asFile();
			FileReader fr = new FileReader(logFile);
			BufferedReader br = new BufferedReader(fr);*/
		BufferedReader stdIn=new BufferedReader(new InputStreamReader(System.in, "utf-8"));
			String line = "";
			
			while ((line = stdIn.readLine()) != null) {
				LogMeta log = parseLog(line);
				if (log == null) {
					continue;
				} 
				
				
				long usrid = log.getUsrid();
				int isNo = PartationUtil.getIsId(usrid+"", lsNum);
				
				if (isNo == slice) {
					
					
					EmlMeta eml = MailParser.parseMessage(log.getId(), log.isIspam());
					if (eml == null) {
						continue;
					}
					MailMeta mailMeta = new MailMeta(eml.getSubject(), eml.getTxtBody()+" "+eml.getHtmlBody(), eml.getFrom(), eml.getTo(), log.getId(), log.getTime(), log.getUsrid(), log.getFolder(), eml.getAttaches(),log.getDocid());
					
					XMLUtil.writeConfig(mailMeta.getUsrid()+"", segPath.cat(mailMeta.getUsrid()+"_" +System.currentTimeMillis()).asFile(), mailMeta);
				}
					
			}
		
		PathUtil.mkdirs(segPath.cat(MailConstants.SEGFINISHED));
		MailParser.close();
		return true;
	}
	
//	private static SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd HH:mm:ss");
	
	@SuppressWarnings("deprecation")
	public static LogMeta parseLog(String line) {
//		System.out.println("parsing :"+line);
		LogMeta log = new LogMeta();
		Map<String, String> params = new HashMap<String, String>();
		
		
		String timestr = line.substring(0,15).trim();
		long date = System.currentTimeMillis();
		
		
		try{
		date = Date.parse(timestr+" "+ (new Date().getYear()+1900) );
		} catch (Exception e) {
			System.out.println(timestr+" "+ (new Date().getYear()+1900) );
			return null;
		}
		
		
		
		
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
		if((act.equals("mta_save_mail")|| act.equals("web_save_mail") || act.equals("mail_size_quota")
				|| act.equals("mail_num_quota") || act.equals("create_user")) 	&& status.indexOf("success")>=0) {
			if (params.get("dirid").equals("5")) {
				return null;
			}
			String usridStr = params.get("usrid");
			
			String dirid = params.get("dirid");
			String msgid = params.get("msgid");
			if (msgid == null) {
				msgid = params.get("x_msgid");
			}
			if (dirid == null) {
				dirid = "1";
			}
			String id = params.get("kvkey");
			
			
			long usrid = MD5Writable.digest(usridStr).halfDigest();
			String docId = params.get("mskey")+"_"+usrid;
			
			log.setFolder(dirid);
			log.setId(Long.parseLong(id));
			log.setTime(date);
			log.setUsrid(usrid);
			log.setMsgid(msgid);
			log.setIspam(false);
			log.setDocid(docId);
			System.out.println("[Success:save_mail] userid="+usrid+" kvkey="+id+" mskey="+docId+" dirid="+dirid);
			return log;
		} else if (act.equals("index_update_dirid")) {
			
			String field = params.get("field");
			if (field != null && field.equals("dirid")) {
				String olddir = params.get("dirid");
				String newdir = params.get("value");
				if (olddir == null || newdir == null) {
					return null;
				}
				String usridStr = params.get("usrid");
				
				String id = params.get("kvkey");
				long usrid = MD5Writable.digest(usridStr).halfDigest();
				String docId = params.get("mskey")+"_"+usrid;
				log.setFolder("2");
				log.setId(Long.parseLong(id));
				log.setTime(date);
				log.setUsrid(usrid);
				log.setMsgid(null);
				log.setDocid(docId);
				if (olddir.equals("5") && newdir.equals("3")) {
					log.setIspam(true);;
				} else {
					log.setIspam(false);
				}
				System.out.println("[Success:update_mail] userid="+usrid+" kvkey="+id+" mskey="+docId+" oldDirid="+olddir+" newDirid"+newdir);
				return log;
			}
			return null;
			
		}else {
			System.out.println("[Error:unkown format] "+line);
			return null;
		}
	}
	
	
	public int slice;
	
	public void testInput() throws IOException {
		BufferedReader stdIn=new BufferedReader(new InputStreamReader(System.in));
		String l = "";
		int count = 0;
		while ((l=stdIn.readLine())!= null ) {
			System.out.println(count + " "+ l );
		}

	}
	
	public static void main(String[] args) throws Exception{
		/*MailDataParseTool tool = new MailDataParseTool();
		
		String basePath = args[0];
		String sliceStr = args[1];
		String lsNumStr = args[2];
		tool.slice = Integer.parseInt(sliceStr);
		tool.setLsNum(Integer.parseInt(lsNumStr));
		
		
		
		try {
			tool.exec(basePath);
//			tool.testInput();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}*/
		
		new LostMailDataParseTool().copyLostLogsFromMss();
	}

}
