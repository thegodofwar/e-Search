package pf.web;


import javax.servlet.ServletConfig;
import javax.servlet.ServletException;

import org.apache.log4j.Logger;
import org.springframework.web.servlet.DispatcherServlet;


public class WanmeiServlet extends DispatcherServlet {
	
	private static final long serialVersionUID = -8612626810771171135L;
	public static final Logger LOG=Logger.getLogger(WanmeiServlet.class.getName());

	@Override
	public void init(ServletConfig config) throws ServletException {
		LOG.info("********WanmeiServlet Initializing*******");
		LOG.info("********WanmeiServlet Initializing*******");
		// Set home dir to {webapp.dir}/imagesearch/WEB-INF/
		String home = config.getServletContext().getRealPath("WEB-INF");
		LOG.info("Home directory=" + home);
		LOG.info("Home directory=" + home);
		// Set wanmei.home/odis.home to home dir
		System.setProperty("wanmei.home", home);
		System.setProperty("nutch.home", home);
		System.setProperty("odis.home", home);
		// Set wordsegment lib dir
		/*System.setProperty("toolbox.wordsegment.lib", home
						+ "/lib/wordsegment");*/
		super.init(config);
	}
}
