package wanmei.utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.InvalidPropertiesFormatException;
import java.util.Properties;
import org.apache.log4j.Logger;

public abstract class AbstractTool {
	public static final Logger LOG=Logger.getLogger(AbstractTool.class.getName());
	
	Properties prop = new Properties();
	public static final String confPath = "conf/";

	public boolean loadConf() throws InvalidPropertiesFormatException, IOException{
		File dir = new File(confPath);
		File[] fs = dir.listFiles();
		
		for (File f : fs) {
			String name = f.getName();
			
			if (name.endsWith(".properties")) {
				LOG.info(name);
				InputStream in = null;
			    
		    	try {
					in = new FileInputStream(confPath+name);
				} catch (FileNotFoundException e) {
					LOG.error("SegInfoFile missed", e);
					return false;
				}
			
				try {
					prop.load(in);
					for (Object s : prop.keySet()) {
						LOG.info(s);
					}
				}  finally {
					in.close();
				}
			}
		}
		return true;
    }
	
}
