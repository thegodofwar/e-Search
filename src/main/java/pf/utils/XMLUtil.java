package pf.utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.InvalidPropertiesFormatException;
import java.util.List;
import java.util.Properties;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.HierarchicalConfiguration;
import org.apache.commons.configuration.XMLConfiguration;
import org.apache.log4j.Logger;
import org.dom4j.Document;
import org.dom4j.DocumentHelper;
import org.dom4j.Element;
import org.dom4j.io.OutputFormat;
import org.dom4j.io.XMLWriter;


import pf.is.data.MailMeta;
import pf.is.data.SegInfo;
import pf.ls.index.MergeMinorIndexTool;
import pf.utils.data.Path;

public class XMLUtil {
	
	public static final Logger LOG=Logger.getLogger(XMLUtil.class.getName());
	public static List<MailMeta> loadMailMetaFromXml(File file) {
		List<MailMeta> result = new ArrayList<MailMeta>();
		
		InputStream is;
		try {
			is = new FileInputStream(file);
		} catch (FileNotFoundException e) {
			LOG.error("usr data can not be found", e);
			return result;
		}
		XMLConfiguration conf = null;
		try {
			conf = new XMLConfiguration();
			conf.setDelimiterParsingDisabled(true);
			conf.load(is, "utf-8");
		} catch (ConfigurationException e) {
			LOG.error( "Load conf ["+file.getAbsolutePath()+":"+file.getName()+"] failed",e);
            return result;
		}
		try{
			long usrid = conf.getLong("[@usr]", -1);
			for (Object mailObj : conf.configurationsAt("mail")) {
	            HierarchicalConfiguration mailConf = (HierarchicalConfiguration) mailObj;
	            String subject = mailConf.getString("subject", "");
	            String from = mailConf.getString("from", "");
	            String to = mailConf.getString("to", "");
	            //String content = mailConf.getString("content", "");
	            String content=mailConf.getString("content", "");
	           // System.out.println("测试content："+content);
	            long id = mailConf.getLong("id", -1);
	            long time = mailConf.getLong("time", -1);
	            String folder = mailConf.getString("folder", "1");
	            String attach = mailConf.getString("attach", "");
	            String docid = mailConf.getString("docid", "0");
	            
	            MailMeta meta = new MailMeta(subject, content, from, to, id, time, usrid, folder, attach, docid);
	            result.add(meta);
			}
		}catch(Exception e){
			LOG.error("get xml content error" + e.getMessage());
		}
		try {
			is.close();
		} catch (IOException e) {
			LOG.error("",e);
		}
		file.delete();
		return result;
	}
	
	public static List<Long> loadMailIdFromXml(File file) {
		List<Long> result = new ArrayList<Long>();
		
		InputStream is;
		try {
			is = new FileInputStream(file);
		} catch (FileNotFoundException e) {
			LOG.error("usr data can not be found", e);
			return result;
		}
		XMLConfiguration conf = null;
		try {
			conf = new XMLConfiguration();
			conf.setDelimiterParsingDisabled(true);
			conf.load(is);
		} catch (ConfigurationException e) {
			LOG.error("Load conf ["+file.getAbsolutePath()+":"+file.getName()+"] failed", e);
            return result;
		}
		
		for (Object mailObj : conf.configurationsAt("mail")) {
            HierarchicalConfiguration mailConf = (HierarchicalConfiguration) mailObj;
            long id = mailConf.getLong("id", -1);
            result.add(id);
		}
		return result;
	}
	
	public static String checkUnicodeString(String value) {
		     char[] chars=value.toCharArray();
			    for (int i=0; i < chars.length; ++i) {
			        if (chars[i] > 0xFFFD) 
			        {
			           chars[i]='\n';//直接替换掉0x0 chars[i]='\n';
			        } 
			        else if (chars[i] < 0x20 && chars[i] != '\t' && chars[i] != '\n' && chars[i] != '\r')
			        {
			            chars[i]='\n';//直接替换掉0x0 chars[i]='\n';
			        }
			    }
			  return new String(chars);
	}
	
	public static void writeConfig(String usrIdStr, File xmlFile, MailMeta... metas) throws IOException {
        Document document = DocumentHelper.createDocument();
        Element root = document.addElement("mails");
        
        root.addAttribute("usr", usrIdStr);
        for (MailMeta meta : metas) {
        	long id = meta.getId();
            String content = meta.getContent();
            String from = meta.getFrom();
            String subject = meta.getSubject();
            String to = meta.getTo();
            String attach = meta.getAttachName();
            String docid = meta.getDocid();
            long time = meta.getTime();
        	Element mailElement = root.addElement("mail");
        	Element idElement = mailElement.addElement("id");
        	idElement.setText(id+"");
        	Element contentElement = mailElement.addElement("content");
        	//contentElement.setText(content);
        	contentElement.addCDATA(checkUnicodeString(content));
        	Element fromElement = mailElement.addElement("from");
        	//fromElement.setText(from);
        	fromElement.addCDATA(checkUnicodeString(from));
        	Element subjectElement = mailElement.addElement("subject");
        	//subjectElement.setText(subject);
        	subjectElement.addCDATA(checkUnicodeString(subject));
        	Element toElement = mailElement.addElement("to");
        	//toElement.setText(to);
        	toElement.addCDATA(checkUnicodeString(to));
        	Element timeElement = mailElement.addElement("time");
        	timeElement.setText(time+"");
        	Element attachElement = mailElement.addElement("attach");
            //attachElement.setText(attach);
        	attachElement.addCDATA(checkUnicodeString(attach));
            Element docidElement = mailElement.addElement("docid");
            docidElement.setText(docid);
        }   
        
        OutputFormat format = OutputFormat.createPrettyPrint();
        format.setEncoding("utf-8");
        XMLWriter writer = new XMLWriter(new FileWriter(xmlFile.getAbsolutePath()), format);
        writer.write(document);
        writer.close();
    }
	
	public static void main(String[] args) {
		/*Path p = new Path("d:\\test\\0\\0");
		List<MailMeta> metas = loadMailMetaFromXml(p.asFile());
		for (MailMeta meta : metas) {
			System.out.println(meta.getContent());
		}*/
		//writeLastTimeBuildMinor(new Date());
		//readLastTimeBuildMinor();
		//System.out.println(readValueStrByKey("ls_ips"));
		//writeSegFile(new SegInfo("2000-12-18",1),new Path("E:/"));
		//System.out.println(new SimpleDateFormat("yyyy-MM-dd HH").format(readLastTimeBuildMinor()));
		//System.out.println(new SimpleDateFormat("yyyy-MM-dd HH").format(readStartTimeBuildMinor()));
		//System.out.println(new SimpleDateFormat("yyyy-MM-dd HH").format(readEndTimeBuildMinor()));
	/*int i=0;
		while(i<100000) {
		LOG.info("这是神马情况！"+i);
		  File a=new File("d:/aa/bb/bbcccc.xml");
		  try {
			InputStream is = new FileInputStream(a);
		} catch (FileNotFoundException e) {
			LOG.error("找不到文件",e);
		}
		  i++;
		  try {
			Thread.sleep(3000);
		} catch (InterruptedException e) {
			LOG.error("线程被打断",e);
		}
	}*/
//loadMailMetaFromXml(new File("E://-2065907905471270992_8_1312254125032"));
loadMailMetaFromXml(new File("/data1/is/crwalsegment/add/2011-08-02/11/-2065907905471270992_8_1312254125032"));
		//writeStartTimeBuildMinor(new Date());
		//writeEndTimeBuildMinor(new Date());
	}

	public static void writeSegFile(SegInfo version, Path path)
			 {
		Properties prop = new Properties();
		prop.put(MergeMinorIndexTool.LOCAL_DIST_VER, version.getDate() + "," + version.getSeg());
		File segInfoFile = path.cat(MailConstants.MINORSEGFILE).asFile();
		if(!segInfoFile.exists()) {
			path.asFile().mkdirs();
			segInfoFile=new File(path.asFile().toString()+"/"+MailConstants.MINORSEGFILE);
		}
		OutputStream xml=null;
		try {
			xml = new FileOutputStream(segInfoFile);
		} catch (FileNotFoundException e) {
			LOG.error("",e);
		}
		try {
			try {
				prop.storeToXML(xml, "Local version");
			} catch (IOException e) {
				LOG.error("",e);
			}
		} finally {
			try {
				xml.close();
			} catch (IOException e) {
				LOG.error("",e);
			}
		}
	}
	
	public static SegInfo readSegFile(Path path) {
		InputStream in = null;
		if (!path.asFile().exists()) {
			path.asFile().mkdirs();
			File segInfoFile=new File(path.asFile().toString()+"/"+MailConstants.MINORSEGFILE);
					XMLUtil.writeSegFile(new SegInfo("1988-12-18",0), path);
				return new SegInfo("1988-12-18",0);
		}
		try {
			in = new FileInputStream(path.cat(MailConstants.MINORSEGFILE).getAbsolutePath());
		} catch (FileNotFoundException e) {
			LOG.info("SegInfoFile missed and creating...");
				XMLUtil.writeSegFile(new SegInfo("1988-12-18",0), path);
			return new SegInfo("1988-12-18",0);
		}
		Properties prop = new Properties();
		try {
			try {
				prop.loadFromXML(in);
			} catch (IOException e) {
				LOG.error("",e);
			}
		} finally {
			try {
				in.close();
			} catch (IOException e) {
				LOG.error("",e);
			}
		}
		String result=prop.getProperty(MergeMinorIndexTool.LOCAL_DIST_VER, "");
		String[] res = result.split(",");
		return new SegInfo(res[0], Integer.parseInt(res[1]));
	}
    
	public static void writeStartTimeBuildMinor(Date date) 
	                            {
		Properties prop = new Properties();
		SimpleDateFormat format=new SimpleDateFormat("yyyy-MM-dd HH");
		String dateStr=format.format(date);
		prop.put(MailConstants.STARTTIME_BUILDMINOR,dateStr);
		//prop.setProperty(tag, dateStr);
		File segInfoFile = new File(MailConstants.STARTFILE);
		OutputStream xml=null;
		try {
			xml = new FileOutputStream(segInfoFile);
		} catch (FileNotFoundException e) {
			LOG.error("",e);
		}
		try {
			try {
				prop.storeToXML(xml, "Parameter configuration");
			} catch (IOException e) {
				LOG.error("",e);
			}
		} finally {
			try {
				xml.close();
			} catch (IOException e) {
				LOG.error("",e);
			}
		}
	}
	
	public static void writeEndTimeBuildMinor(Date date) 
                                {
       Properties prop = new Properties();
       SimpleDateFormat format=new SimpleDateFormat("yyyy-MM-dd HH");
       String dateStr=format.format(date);
       prop.put(MailConstants.ENDTIME_BUILDMINOR,dateStr);
       //prop.setProperty(tag, dateStr);
       File segInfoFile = new File(MailConstants.ENDFILE);
       OutputStream xml=null;
	try {
		xml = new FileOutputStream(segInfoFile);
	} catch (FileNotFoundException e) {
		LOG.error("",e);
	}
       try {
            try {
				prop.storeToXML(xml, "Parameter configuration");
			} catch (IOException e) {
				LOG.error("",e);
			}
        } finally {
             try {
				xml.close();
			} catch (IOException e) {
				LOG.error("",e);
			}
         }
}
	
	public static Date readStartTimeBuildMinor() 
	                         {
		InputStream in = null;
	    try {
			in = new FileInputStream(new File(MailConstants.STARTFILE));
		} catch (FileNotFoundException e) {
			LOG.error("",e);
		}
		Properties prop = new Properties();
		try {
			try {
				prop.loadFromXML(in);
			} catch (InvalidPropertiesFormatException e) {
				LOG.error("",e);
			} catch (IOException e) {
				LOG.error("",e);
			}
		} finally {
			try {
				in.close();
			} catch (IOException e) {
				LOG.error("",e);
			}
		}
		String result = prop.getProperty(MailConstants.STARTTIME_BUILDMINOR, "");
		SimpleDateFormat format=new SimpleDateFormat("yyyy-MM-dd HH");
		Date d=null;
		try {
			d = format.parse(result);
		} catch (ParseException e) {
			LOG.error("",e);
		}
		return d;
	}
	
	public static Date readEndTimeBuildMinor()
	                     {
		InputStream in = null;
	    try {
			in = new FileInputStream(new File(MailConstants.ENDFILE));
		} catch (FileNotFoundException e) {
			LOG.error("",e);
		}
		Properties prop = new Properties();
		try {
			try {
				prop.loadFromXML(in);
			} catch (InvalidPropertiesFormatException e) {
				LOG.error("",e);
			} catch (IOException e) {
				LOG.error("",e);
			}
		} finally {
			try {
				in.close();
			} catch (IOException e) {
				LOG.error("",e);
			}
		}
		String result = prop.getProperty(MailConstants.ENDTIME_BUILDMINOR, "");
		SimpleDateFormat format=new SimpleDateFormat("yyyy-MM-dd HH");
		Date d=null;
		try {
			d = format.parse(result);
		} catch (ParseException e) {
			LOG.error("",e);
		}
		return d;
	}
	
	public static String readValueStrByKey(String key) 
	                            {
		InputStream in = null;
		LOG.info("path is " + XMLUtil.class.getResource("/").getPath());
	    String realPath = XMLUtil.class.getResource("/").getPath() + MailConstants.CONFIG;
	    try {
			in = new FileInputStream(new File(realPath));
		} catch (FileNotFoundException e) {
			LOG.error("没有找到文件",e);
			return null;
		}
		Properties prop = new Properties();
		try {
			try {
				prop.loadFromXML(in);
			} catch (InvalidPropertiesFormatException e) {
				LOG.error("",e);
			} catch (IOException e) {
				LOG.error("",e);
			}
		} finally {
			try {
				in.close();
			} catch (IOException e) {
				LOG.error("",e);
			}
		}
		String value = prop.getProperty(key, "");
		return value; 	
	}
	
	public static String readDSConfig(String key) {
		InputStream in = null;
		LOG.info("path is " + XMLUtil.class.getClassLoader().getResource("/"));
	    in = XMLUtil.class.getClassLoader().getResourceAsStream(MailConstants.CONFIG);
		Properties prop = new Properties();
		try {
			try {
				prop.loadFromXML(in);
			} catch (InvalidPropertiesFormatException e) {
				LOG.error("",e);
			} catch (IOException e) {
				LOG.error("",e);
			}
		} finally {
			try {
				in.close();
			} catch (IOException e) {
				LOG.error("",e);
			}
		}
		String value = prop.getProperty(key, "");
		return value; 	
	}
	
}
