package pf.is.data;

public class LogMeta {
	
	public long id;
	public long time;
	public long usrid;
	public String folder;
	public boolean ispam;
	public String msgid;
	public String docid;
	
	public LogMeta() {
		this.id = 0L;
		this.time = -1;
		this.usrid = -1;
		this.folder = "";
		this.ispam = false;
		this.msgid = "";
		this.docid = "";
	}

	public long getId() {
		return id;
	}

	public void setId(long id) {
		this.id = id;
	}

	public long getTime() {
		return time;
	}

	public void setTime(long time) {
		this.time = time;
	}

	public long getUsrid() {
		return usrid;
	}

	public void setUsrid(long usrid) {
		this.usrid = usrid;
	}

	public String getFolder() {
		return folder;
	}

	public void setFolder(String folder) {
		this.folder = folder;
	}
	
	public boolean isIspam() {
		return ispam;
	}

	public void setIspam(boolean ispam) {
		this.ispam = ispam;
	}

	public String getMsgid() {
		return msgid;
	}

	public void setMsgid(String msgid) {
		this.msgid = msgid;
	}
	
	public String getDocid() {
		return docid;
	}

	public void setDocid(String docid) {
		this.docid = docid;
	}

	public String toString() {
		return "id:"+id + " time:" + time + " usrid:" + usrid + " folder:" + folder + " ispam:" 
		+ ispam + "msgid:"+ msgid;
		
	}
	
	

}
