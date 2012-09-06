package pf.is.tool;


public class TestMailDataParseTool {
	/*
	public static final String SEGMENT = "crwalsegment";
	public static final String ADD = "add";
	public static final String DEL = "del";
	public static final String EMLBASEPATH = "/data1/pwmail";
	public static final int BUFFERSIZE = 1024;
	
	
	public boolean exec(String basePathStr, String logStr) throws IOException {
		System.out.println("start");
		//create segment
		Path basePath = new Path(basePathStr);
		String day = DateUtil.genCurrentDayStr();
		Path dayPath = basePath.cat(SEGMENT).cat(ADD).cat(day);
		
		PathUtil.mkdirs(dayPath);
		Segments segs = new Segments(dayPath);
		int newseg = segs.createNewSegment();
		Path segPath = dayPath.cat(newseg+"");
		
		PooledMailParser parser = new PooledMailParser();
		
		Path spamPath = new Path("/data1/spam");
		Path[] emls = spamPath.listPathes();
//		List<MailMeta> metaBuffer = new ArrayList<MailMeta>();
		int count = 0;
		for (Path emlfile : emls) {
			count ++;
			
				
				EmlMeta eml = parser.parse(emlfile.getAbsolutePath());
				MailMeta mailMeta = new MailMeta(eml.getSubject(), eml.getTxtBody()+eml.getHtmlBody(), eml.getFrom(), eml.getTo(), count, System.currentTimeMillis(), MD5Writable.digest(eml.getTo()).halfDigest(), "1", eml.getAttaches());
//				metaBuffer.add(mailMeta);
				
//				if (metaBuffer.size() >= BUFFERSIZE) {
				XMLUtil.writeConfig(mailMeta.getUsrid()+"", segPath.cat(mailMeta.getUsrid()+"_" +System.currentTimeMillis()).asFile(), mailMeta);
//			
			
		}
		
		PathUtil.mkdirs(segPath.cat(MailConstants.SEGFINISHED));
		return true;
	}
	
	private static LogMeta parseLog(String line) {
		LogMeta log = new LogMeta();
		Map<String, String> params = new HashMap<String, String>();
		String[] subs = line.split("\\s+");
		for (String sub : subs) {
			String[] kv = sub.split("=");
			if (kv != null && kv.length == 2) {
				params.put(kv[0], kv[1]);
			}
		}
		
		if (!params.containsKey("act") || !params.containsKey("stat")) {
			return null;
		}
		String act = params.get("act");
		String status = params.get("stat");
		if(act.equals("save_mail") && status.equals("success")) {
			String usridStr = params.get("usrid");
			String emlPath = params.get("emlpath");
			String dirid = params.get("dirid");
			if (dirid == null) {
				dirid = "1";
			}
			String id = params.get("key");
			
			long usrid = MD5Writable.digest(usridStr).halfDigest();
			if (emlPath == null) {
				return null;
			}
			log.setEmailPath(EMLBASEPATH + emlPath.substring(19).replace("emlpath", ""));
			
			log.setFolder(dirid);
			log.setId(id);
			log.setTime(System.currentTimeMillis());
			log.setUsrid(usrid);
			return log;
		} else {
			return null;
		}
	}
	
	
	
	public static void main(String[] args) {
		TestMailDataParseTool tool = new TestMailDataParseTool();
		
		String basePath = args[0];
		String logStr = args[1];
		
		try {
			tool.exec(basePath, logStr);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
	}*/

}
