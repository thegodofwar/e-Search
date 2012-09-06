package pf.utils;

public class MailConstants {
	public static final int INDEXSERVICE = 0;
	public static final String FOLDER_ALL = "0";
	public static final String FOLDER_INBOX = "1";
	public static final String FOLDER_SENDBOX = "2";
	
	public static final String FILED_ALL = "0";
	public static final String FILED_COTENT = "1";
	public static final String FILED_SUBJECT = "2";
	public static final String FILED_FROM = "3";
	public static final String FILED_TO = "4";
	
	
	// segment
	public static final String DAY_FINISHED = "finished";
	public static final String BUILT_TAG = "built";
	public static final String CANBEISSUE_TAG = "canbeissue";
	public static final String ISSUE_TAG = "issue";
	public static final String MAJOR_TAG = "major";
	public static final String UPDATED_TAG = "updated";
	
	
	
	//index
	public static final String CONTENT_FIELD = "content";
	public static final String SUBJECT_FIELD = "subject";
	public static final String FROM_FIELD = "from";
	public static final String TO_FIELD = "to";
	public static final String TIME_FIELD = "time";
	public static final String USR_FIELD = "usrid";
	public static final String ATTACH_FIELD = "attach";
	public static final String HASATT_FIELD = "hasatt";

	//LS
	public static final String MINORSEGFILE = "seginfo.xml";
	public static final String MERGED_TAG = "merged";
	public static final String PART_PRE = "part-";
	public static final int QUEUE_MAX = 10000;//the number of a queue storing preparing for indexing mails
	public static final int DELETE_INDEX_CACHE=10000;//the max number for deleting index according to analyze log at once

	//DS
	public static final int DS_TTL = 3000;
	public static final String QUERY_FILED = "q";
	public static final String START_FILED = "s";
	public static final String LENGTH_FILED = "l";
	public static final String CONTENT_QUERY = "ct";
	public static final String From_QUERY = "fr";
	public static final String TO_QUERY = "to";
	public static final String SUBJECT_QUERY = "sub";
	public static final String ATTACH_QUERY = "att";
	public static final String ATTACH_NAMES = "att_names";
	public static final String DELETE_DOCID = "docId";
	public static final String DELETE_DOCIDS = "docIds";
	

	public static final String SORTTYPE = "st";

	public static final String STARTTIME_FILED = "stime";
	public static final String ENDTIME_FILED = "etime";
	public static final String FORLDER_FIELD = "fo";
	public static final String SEARCHFIELD_FIELD = "fd";
	public static final String SEGFINISHED = "segfinished";
	public static final String OPERATOR = "op";
	public static final String OR = "or";
	public static final String AND = "and";
	public static final String CONFIG="config.xml";
	public static final String STARTFILE="startTime_buildMinor.xml";
	public static final String ENDFILE="endTime_buildMinor.xml";
	public static final String STARTTIME_BUILDMINOR="startTime_buildMinor";
 	public static final String ENDTIME_BUILDMINOR="endTime_buildMinor";
 	public static final long MAX_MAILCONTENT_LENGTH=1024*1024;//最大邮件内容大小为1M
 	public static final String INDEXFOLDER="indexFolder";

	
	
	
	

}
