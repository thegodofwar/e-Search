package pf.is.parser;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.james.mime4j.codec.DecodeMonitor;
import org.apache.james.mime4j.codec.DecoderUtil;
import org.apache.james.mime4j.dom.Entity;
import org.apache.james.mime4j.dom.Message;
import org.apache.james.mime4j.dom.Multipart;
import org.apache.james.mime4j.dom.TextBody;
import org.apache.james.mime4j.dom.address.Mailbox;
import org.apache.james.mime4j.dom.address.MailboxList;
import org.apache.james.mime4j.message.BodyPart;
import org.apache.james.mime4j.message.DefaultMessageBuilder;
import org.apache.james.mime4j.stream.Field;
import org.apache.james.mime4j.stream.MimeConfig;
import org.apache.james.mime4j.util.MimeUtil;
import org.apache.log4j.Logger;

import pf.is.data.EmlMeta;
import pf.is.data.MailMeta;
import pf.is.kv.KVclient;
import pf.is.kv.KVclient.kv_client_mem;
import pf.utils.MailConstants;
import pf.utils.XMLUtil;

import com.sun.jna.Native;
import com.sun.jna.Pointer;
import com.sun.jna.ptr.PointerByReference;

/**
 * 
 * @author liufukun
 */

public class MailParser {
	public static final Logger LOG=Logger.getLogger(MailParser.class.getName());

	// private static Log log = LogFactory.getLog(MailParser.class);
	public static int KV_EMAIL_TYPE = 0;
	public static int KV_SPAM_TYPE = 1;

	
	private static KVclient kv;
	public static void init() {
		LOG.info("start int kv...");
		Native.loadLibrary("xerces-c-3.1", KVclient.class);
		Native.loadLibrary("xerces-c", KVclient.class);
		kv = (KVclient) Native.loadLibrary("kv_client", KVclient.class);
		LOG.info("start init");
		String realPath = MailParser.class.getResource("/").getPath() + "example.xml";
		int res= kv.kv_client_init(realPath);
		LOG.info("inited");
		if (res != 0) {
			LOG.info("init kv_client error:" + res);
		}
	}
	static {
		init();
	}
	
		
	
	public MailParser() {

	}
	
	
	
	private static String getEmailContent(long key, int type) {
		String res = "";
		PointerByReference pp = new PointerByReference();
		int rres = 	kv.kv_client_read(key, type, pp);
		
		if (rres == 0) {
			Pointer p = pp.getValue();
			kv_client_mem mem = new kv_client_mem(p);
			
			mem.read();
			
			res = new String(mem.mem_ptr.getByteArray(0, mem.mem_size), 0, mem.mem_size);
			kv.release_data_buffer(pp.getValue());
		}
		return res;
	}

	public static EmlMeta parseMessage(long key, boolean isSpam) {
		LOG.info("emlkey:"+key+" isSpam:"+isSpam);
		StringBuffer txtBody;
		StringBuffer htmlBody;
		ArrayList<BodyPart> attachments;
		
		EmlMeta meta = new EmlMeta();
		String msg;
		if (isSpam) {
			msg = getEmailContent(key, KV_SPAM_TYPE);
		} else {
			msg = getEmailContent(key, KV_EMAIL_TYPE);
		}
		
		if (msg == null || msg.trim().length() <=0 ) {
			LOG.info("[Error:read_mail] key="+key);
			return null;
		}
		
		txtBody = new StringBuffer();
		htmlBody = new StringBuffer();
		attachments = new ArrayList<BodyPart>();
		InputStream is = null;
		DefaultMessageBuilder build = new DefaultMessageBuilder();
		MimeConfig conf = new MimeConfig();
		conf.setMaxLineLen(1000000);
		conf.setMaxHeaderLen(1000000);
		build.setMimeEntityConfig(conf);
		try {
			is = new ByteArrayInputStream(msg.getBytes());

			Message mimeMsg = build.parseMessage(is);
			String subject = mimeMsg.getSubject();
			
			String from = "";
			try{
				MailboxList fromOb = mimeMsg.getFrom();
				if (fromOb != null) {
					for(int i=0; i<fromOb.size(); i++){
						Mailbox mb = fromOb.get(i);
						String name = mb.getName();
						String address = mb.getAddress();
						if(name != null && name.length() > 0){
							from += DecoderUtil.decodeEncodedWords(name, DecodeMonitor.SILENT);
							from += " ";
						}
						
						if(address != null && address.length() > 0){
							from += address;
							from += " ";
						}
					}
				}
			}catch (Exception ex) {
				LOG.error("[Parse From Error key:]"+key+" Ex=", ex);
			}

			String to = "";
			try{
				if(mimeMsg.getTo() != null){
					MailboxList toOb = mimeMsg.getTo().flatten();
					if (toOb != null) {
						for(int i=0; i<toOb.size(); i++){
							Mailbox mb = toOb.get(i);
							String name = mb.getName();
							String address = mb.getAddress();
							if(name != null && name.length() > 0){
								to += DecoderUtil.decodeEncodedWords(name, DecodeMonitor.SILENT);
								to += " ";
							}
							
							if(address != null && address.length() > 0){
								to += address;
								to += " ";
							}
						}
					}
				}
			}catch (Exception ex) {
				LOG.error("[Parse To Error key:]"+key+" Ex=", ex);
			}
			
			try{
				if(mimeMsg.getCc() != null){
					MailboxList ccOb = mimeMsg.getCc().flatten();
					if (ccOb != null) {
						for(int i=0; i<ccOb.size(); i++){
							Mailbox mb = ccOb.get(i);
							String name = mb.getName();
							String address = mb.getAddress();
							if(name != null && name.length() > 0){
								to += DecoderUtil.decodeEncodedWords(name, DecodeMonitor.SILENT);
								to += " ";
							}
							
							if(address != null && address.length() > 0){
								to += address;
								to += " ";
							}
						}
					}
				}
			}catch (Exception ex) {
				LOG.error("[Parse CC Error key:]"+key+" Ex=", ex);
			}

			meta.setFrom(from);
			meta.setTo(to);
			meta.setSubject(subject);
			LOG.info("parse message get from:" + from + " to:" + to + " subject:" + subject);

			// Get custom header by name
			/*Field priorityFld = mimeMsg.getHeader().getField("X-Priority");
			// If header doesn't found it returns null
			if (priorityFld != null) {
				// Print header value
				LOG.info("Priority: " + priorityFld.getBody());
			}*/

			// If message contains many parts - parse all parts
			if (mimeMsg.isMultipart()) {
				Multipart multipart = (Multipart) mimeMsg.getBody();
				parseBodyParts(multipart, txtBody, htmlBody, attachments,key);
			} else {
				// If it's single part message, just get text body
				String text = getTxtPart(mimeMsg,key);
				if(text != null)
					txtBody.append(text);
			}

			String txt = Html2Text(txtBody.toString());
			String html = Html2Text(htmlBody.toString());

			meta.setTxtBody(txt);
			meta.setHtmlBody(html);

			try{
				for (BodyPart attach : attachments) {
					if(attach.getFilename() != null){
						String attName = DecoderUtil.decodeEncodedWords(attach.getFilename(), DecodeMonitor.SILENT);
						LOG.info("parese message get attach:" + attName);
						meta.addAttach(attName);
					}
				}
			}catch (Exception ex) {
				LOG.error("[Parse Attachment Error key:]"+key+" Ex=", ex);
			}

			/*
			 * for (BodyPart attach : attachments) { String attName =
			 * attach.getFilename(); //Create file with specified name
			 * FileOutputStream fos = new FileOutputStream(attName); try { //Get
			 * attach stream, write it to file BinaryBody bb = (BinaryBody)
			 * attach.getBody(); bb.writeTo(fos); } finally { fos.close(); } }
			 */

		} catch (IOException ex) {
			LOG.error("[Error key:]"+key+" Ex=", ex);
		} catch (Exception ex1) {
			LOG.error("[Error key:]"+key+" Ex1=",ex1);	
		} finally {
			if (is != null) {
				try {
					is.close();
				} catch (IOException ex) {
					LOG.info("[Error key:]"+key+" finally=", ex);
				}
			}
		}
		LOG.info("[Success key:]"+key);
		return meta;

	}
	/**
	 * 
	 * @param fileName
	 */
	public EmlMeta parseMessage(String file) {

		StringBuffer txtBody;
		StringBuffer htmlBody;
		ArrayList<BodyPart> attachments;
		
		
		FileInputStream fis = null;
		EmlMeta meta = new EmlMeta();

		txtBody = new StringBuffer();
		htmlBody = new StringBuffer();
		attachments = new ArrayList<BodyPart>();

		try {
			// Get stream from file
			fis = new FileInputStream(file);
			// Create message with stream from file
			// If you want to parse String, you can use:
			// Message mimeMsg = new Message(new
			// ByteArrayInputStream(mimeSource.getBytes()));
			DefaultMessageBuilder build = new DefaultMessageBuilder();
			Message mimeMsg = build.parseMessage(fis);

			// Get some standard headers
			String subject = mimeMsg.getSubject();
			MailboxList fromOb = mimeMsg.getFrom();
			String from = "";
			if (fromOb != null) {
				for(int i=0; i<fromOb.size(); i++){
					Mailbox mb = fromOb.get(i);
					String name = mb.getName();
					String address = mb.getAddress();
					if(name != null && name.length() > 0){
						from += DecoderUtil.decodeEncodedWords(name, DecodeMonitor.SILENT);
						from += " ";
					}
					
					if(address != null && address.length() > 0){
						from += address;
						from += " ";
					}
				}
			}

			String to = "";
			MailboxList toOb = mimeMsg.getTo().flatten();
			if (toOb != null) {
				for(int i=0; i<toOb.size(); i++){
					Mailbox mb = toOb.get(i);
					String name = mb.getName();
					String address = mb.getAddress();
					if(name != null && name.length() > 0){
						to += DecoderUtil.decodeEncodedWords(name, DecodeMonitor.SILENT);
						to += " ";
					}
					
					if(address != null && address.length() > 0){
						to += address;
						to += " ";
					}
				}
			}
			
			MailboxList ccOb = mimeMsg.getCc().flatten();
			if (ccOb != null) {
				for(int i=0; i<ccOb.size(); i++){
					Mailbox mb = ccOb.get(i);
					String name = mb.getName();
					String address = mb.getAddress();
					if(name != null && name.length() > 0){
						to += DecoderUtil.decodeEncodedWords(name, DecodeMonitor.SILENT);
						to += " ";
					}
					
					if(address != null && address.length() > 0){
						to += address;
						to += " ";
					}
				}
			}

			meta.setFrom(from);
			meta.setTo(to);
			meta.setSubject(subject);
			LOG.info("parse message get from:" + from + " to:" + to + " subject:" + subject);

			// Get custom header by name
			Field priorityFld = mimeMsg.getHeader().getField("X-Priority");
			// If header doesn't found it returns null
			if (priorityFld != null) {
				// Print header value
				LOG.info("Priority: " + priorityFld.getBody());
			}

			// If message contains many parts - parse all parts
			if (mimeMsg.isMultipart()) {
				Multipart multipart = (Multipart) mimeMsg.getBody();
				parseBodyParts(multipart, txtBody, htmlBody, attachments,0);
			} else {
				// If it's single part message, just get text body
				String text = getTxtPart(mimeMsg,0);
				if(text != null)
					txtBody.append(text);
			}

			String txt = Html2Text(txtBody.toString());
			String html = Html2Text(htmlBody.toString());

			meta.setTxtBody(txt);
			meta.setHtmlBody(html);

			for (BodyPart attach : attachments) {
				if(attach.getFilename() != null){
					String attName = DecoderUtil.decodeEncodedWords(attach.getFilename(), DecodeMonitor.SILENT);
					LOG.info("parese message get attach:" + attName);
					meta.addAttach(attName);
				}
			}

			
			  
			 

		} catch (IOException ex) {
			ex.fillInStackTrace();
		} finally {
			if (fis != null) {
				try {
					fis.close();
				} catch (IOException ex) {
					LOG.error("",ex);
				}
			}
		}

		return meta;
	}

	/**
	 * This method classifies bodyPart as text, html or attached file
	 * 
	 * @param multipart
	 * @throws IOException
	 */
	private static void parseBodyParts(Multipart multipart, StringBuffer txtBody, StringBuffer htmlBody, ArrayList<BodyPart> attachments,long key){
		try{
			for (Entity entry : multipart.getBodyParts()) {
				BodyPart part  = (BodyPart)entry;
				if (part.isMimeType("text/plain")) {
	
					String txt = getTxtPart(part,key);
					if(txt != null)
						txtBody.append(txt);
				} else if (part.isMimeType("text/html")) {
	
					String html = getTxtPart(part,key);
					if(html != null)
						htmlBody.append(html);
				} else if (part.getDispositionType() != null
						&& part.getDispositionType().toLowerCase().equals("attachment")) {
					// If DispositionType is null or empty, it means that it's
					// multipart, not attached file
					attachments.add(part);
				}else if(part.getMimeType().toLowerCase().contains("rfc822")){
					part.setFilename("");
					attachments.add(part);
				}
	
				// If current part contains other, parse it again by recursion
				if (part.isMultipart()) {
					parseBodyParts((Multipart) part.getBody(), txtBody, htmlBody, attachments ,key);
				}
			}
		}catch (Exception ex) {
			LOG.error("[Parse Multipart Error key:]"+key+" Ex="+ex.getMessage());
		}
	}

	/**
	 * 
	 * @param part
	 * @return
	 * @throws IOException
	 */
	private static String getTxtPart(Entity part,long key){
		// Get content from body
		try{
			String encode = part.getCharset();
			if(encode.toLowerCase().startsWith("gb"))
				encode = "GB18030";
			TextBody tb = (TextBody) part.getBody();
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			tb.writeTo(baos);
			return new String(baos.toByteArray(), encode);
		}catch(IOException e){
			LOG.error("[Parse Txt Error key:]"+key+" Ex="+e.getMessage());
			return null;
		}
	}

	private static final String regEx_script = "<[\\s]*?script[^>]*?>[\\s\\S]*?<[\\s]*?\\/[\\s]*?script[\\s]*?>"; // 定义script的正则表达式{或<script[^>]*?>[\\s\\S]*?<\\/script>
	private static final String regEx_style = "<[\\s]*?style[^>]*?>[\\s\\S]*?<[\\s]*?\\/[\\s]*?style[\\s]*?>"; // 定义style的正则表达式{或<style[^>]*?>[\\s\\S]*?<\\/style>
	private static final String regEx_html = "<[^>]+>"; // 定义HTML标签的正则表达式
	private static final String regEx_nbsp = "&nbsp;";
	private static final java.util.regex.Pattern p_script = Pattern.compile(
			regEx_script, Pattern.CASE_INSENSITIVE);
	private static final java.util.regex.Pattern p_style = Pattern.compile(
			regEx_style, Pattern.CASE_INSENSITIVE);
	private static final java.util.regex.Pattern p_html = Pattern.compile(
			regEx_html, Pattern.CASE_INSENSITIVE);
	private static final java.util.regex.Pattern p_nbsp = Pattern.compile(
			regEx_nbsp, Pattern.CASE_INSENSITIVE);

	// TODO:the result will not be correct in the case like <哈哈>
	public static String Html2Text(String inputString) {
		String htmlStr = inputString; // 含html标签的字符串
		String textStr = "";
		try {

			Matcher m_script = p_script.matcher(htmlStr);
			htmlStr = m_script.replaceAll(""); // 过滤script标签

			Matcher m_style = p_style.matcher(htmlStr);
			htmlStr = m_style.replaceAll(""); // 过滤style标签

			Matcher m_html = p_html.matcher(htmlStr);
			htmlStr = m_html.replaceAll(""); // 过滤html标签

			Matcher m_nbsp = p_nbsp.matcher(htmlStr);
			htmlStr = m_nbsp.replaceAll("");

			textStr = htmlStr;

		} catch (Exception e) {
			LOG.error("Html2Text Error: " + e.getMessage());
		}

		return textStr;// 返回文本字符串
	}

	/**
	 * 
	 * @param args
	 */
	public static void main(String[] args) {

		String eml1 = args[0];

		MailParser parser = new MailParser();
		EmlMeta eml = parser.parseMessage(eml1);
		MailMeta mailMeta = new MailMeta(eml.getSubject(), eml.getTxtBody()+" "+eml.getHtmlBody(), eml.getFrom(), eml.getTo(), 111, 222, 333, "", eml.getAttaches(), "");
		try {
			XMLUtil.writeConfig("111", new File(args[1]), mailMeta);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			LOG.error("",e);
		}

		/*
		 * MailParser parser = new MailParser(); String ss =
		 * parser.Html2Text("<哈哈>"); System.out.println(ss);
		 */

	}



	public static void close() {
//		kv.kv_client_destroy();
	}



	
}
