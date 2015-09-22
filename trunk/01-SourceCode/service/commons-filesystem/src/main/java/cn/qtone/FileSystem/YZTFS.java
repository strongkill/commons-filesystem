package cn.qtone.FileSystem;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;

public class YZTFS {
	public static String Hadoopfsotype = "hadoopFSO";//hadoop //wcs//qinuiu
	public static String Qiniufsotype = "qiniu";
	public static String WCSfsotype="wcs";
	public static String DEFAULTFSTYPE = Hadoopfsotype;

	
	
	public static final String[] TinyFileNameList = new String[] {
		"1", "2", "3", "4", "5", "6", "7", "8", "9", "0", "a", "b", "c",
		"d",
		"e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "p", "q",
		"r",
		"s", "t", "u", "v", "w", "x", "y", "z" , "A", "B", "C", "D", "E",
		"F",
		"G", "H", "I", "J", "K", "L", "M", "N", "O", "P", "Q", "R", "S",
		"T",
		"U", "V", "W", "X", "Y", "Z", "_"};
	

	/**
	 * auto create folder with date format: yyyyMM/dd
	 * 
	 * @return str_file
	 */
	public static String createAutoDirs() {
		Date now_date = new Date();
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
		String str_date = sdf.format(now_date);
		int len = str_date.length();

		String str_dd = "01";
		if (len == 8)
			str_dd = str_date.substring(6, 8);
		if (len > 6)
			str_date = str_date.substring(0, 6);

		// String sys_separator = System.getProperty("file.separator");

		//String str_file = File.separator + str_date + File.separator + str_dd + File.separator;

		String str_file = "/" + str_date + "/" + str_dd + "/";

		return str_file;
	}
	public static String getRandomTinyFilename(int num){
		String Passwd = "";
		Random rand = new Random();

		for (int j = 0; j < num; j++) {
			int i = rand.nextInt();
			i = rand.nextInt(TinyFileNameList.length);
			Passwd += TinyFileNameList[i];
		}
		return Passwd;
	}	


	/**
	 * generator UUID FileName with fileName ext 
	 * 
	 * 
	 * @param fileName
	 *            
	 * @return generator Filename
	 */
	public static String getRamdomFileName(String fileName) {
		String extName = getExt(fileName);
		String result = java.util.UUID.randomUUID().toString();
		//int i = (int) (Math.random() * 1000 );
		//long time = System.currentTimeMillis();
		//long result = time * 1000 + i;
		//String result = time + getRandomTinyFilename(12) + extName;
		return result+extName;
	}
	/**
	 * 
	 * 
	 * @param fileName
	 *          
	 * @return extName
	 */
	public static String getExt(String fileName) {
		int pos = fileName.lastIndexOf(".");
		String extName = ".txt";
		if (pos > -1)
			extName = fileName.substring(pos, fileName.length());
		if(".bmp".equalsIgnoreCase(extName))
			extName=".jpg";
		return extName;
	}	
}
