package pf.utils;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.Map;
import java.util.Map.Entry;

import javax.servlet.http.HttpServletRequest;

public class ParamUtil {

	public static int getInt(HttpServletRequest request, String name, int def) {
		String tmp = request.getParameter(name);
		if (tmp == null || tmp.length() == 0)
			return def;
		else {
			try {
				return Integer.parseInt(tmp.trim());
			} catch (NumberFormatException e) {
				return def;
			}
		}
	}

	public static long getLong(HttpServletRequest request, String name, long def) {
		String tmp = request.getParameter(name);
		if (tmp == null || tmp.length() == 0)
			return def;
		else {
			try {
				return Long.parseLong(tmp.trim());
			} catch (NumberFormatException e) {
				return def;
			}
		}
	}

	public static String getUTF8String(HttpServletRequest request, String name,
			String def) {
		String tmp = request.getParameter(name);
		if (tmp == null)
			return def;
		else {
			try {
				return URLDecoder.decode(tmp, "utf-8");
			} catch (UnsupportedEncodingException e) {
				return def;
			}
		}
	}

	public static String getString(HttpServletRequest request, String name,
			String def) {
		String tmp = request.getParameter(name);
		if (tmp == null)
			return def;
		else {
			return tmp;
		}
	}

	@SuppressWarnings("unchecked")
	public static Map<String, String[]> getAllParameters(
			HttpServletRequest request) {
		return request.getParameterMap();
	}

	public static String getParametersString(Map<String, String[]> paras) {
		StringBuffer sb = new StringBuffer();
		for (Entry<String, String[]> e : paras.entrySet()) {
			String k = e.getKey();
			for (String v : e.getValue()) {
				sb.append("&");
				sb.append(k);
				sb.append("=");
				sb.append(v);
			}
		}
		return sb.toString();
	}

}
