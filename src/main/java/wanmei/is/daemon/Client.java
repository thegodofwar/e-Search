package wanmei.is.daemon;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;

import org.apache.log4j.Logger;

import wanmei.utils.XMLUtil;

public class Client {
	public static final Logger LOG=Logger.getLogger(XMLUtil.class.getName());
	// 网上抄来的，将 int 转成字节
	public static byte[] i2b(int i) {
		return new byte[] { (byte) ((i >> 24) & 0xFF),
				(byte) ((i >> 16) & 0xFF), (byte) ((i >> 8) & 0xFF),
				(byte) (i & 0xFF) };
	}

	/**
	 * 发送文件。文件大小不能大于 {@link Integer#MAX_VALUE}
	 * 
	 * @param hostname
	 *            接收端主机名或 IP 地址
	 * @param port
	 *            接收端端口号
	 * @param filepath
	 *            文件路径
	 * 
	 * @throws IOException
	 *             如果读取文件或发送失败
	 */
	public void sendFile(String hostname, int port, String filepath)
			throws IOException {
		File file = new File(filepath);
		FileInputStream is = new FileInputStream(filepath);

		Socket socket = new Socket(hostname, port);
		OutputStream os = socket.getOutputStream();

		try {
			int length = (int) file.length();
			LOG.info("发送文件：" + file.getName() + "，长度：" + length);

			// 发送文件名和文件内容
			writeFileName(file, os);
			writeFileContent(is, os, length);
		} finally {
			os.close();
			is.close();
		}
	}

	// 输出文件内容
	private void writeFileContent(InputStream is, OutputStream os, int length)
			throws IOException {
		// 输出文件长度
		os.write(i2b(length));

		// 输出文件内容
		byte[] buffer = new byte[4096];
		int size;
		while ((size = is.read(buffer)) != -1) {
			os.write(buffer, 0, size);
		}
	}

	// 输出文件名
	private void writeFileName(File file, OutputStream os) throws IOException {
		byte[] fn_bytes = file.getName().getBytes();

		os.write(i2b(fn_bytes.length)); // 输出文件名长度
		os.write(fn_bytes); // 输出文件名
	}

	public static void main(String[] args) throws Exception {
		int port = 7788;
		new DataReceiveDaemon(port, "c:\\save\\").start();
		new Client().sendFile("127.0.0.1", port, "c:\\迷失在康熙末年.txt");
	}
}
