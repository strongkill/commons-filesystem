package cn.qtone.FileSystem;

import java.io.InputStream;

import org.apache.commons.lang.StringUtils;

import cn.qtone.FileSystem.entry.MyRet;

import com.chinanetcenter.api.domain.HttpClientResult;
import com.chinanetcenter.api.domain.PutPolicy;
import com.chinanetcenter.api.util.Config;
import com.chinanetcenter.api.wsbox.FileDownCommand;
import com.chinanetcenter.api.wsbox.FileManageCommand;
import com.chinanetcenter.api.wsbox.FileUploadCommand;

/*
 * 网址：https://portal.chinanetcenter.com/cas/login
用户名：qtone
密码：qtone123!@#
 */
public class WCS   extends YZTFS {
	static String ak = "14ec9b004c5419bb54722c24909388c3fd65dfc5";
	static String sk = "8477987f627ad74a9435e5d5f2407afb81c0edc1";
	static String bucketName = "yzt-album";
	static String returnBody = "bucket=$(bucket)&key=$(key)&fname=$(fname)&fsize=$(fsize)&url=$(url)&hash=$(hash)"; // 自定义返回信息
	public static String domainhosts = "http://113.107.112.147/yzt-album.s.wcsapi.biz.matocloud.com";
	
	static {
		Config.init(ak, sk);
	}
	
	/**
	 * Tested
	 * @return
	 */
	public static String createAutoWCSDir(){
		return createAutoDirs();
	}

	/**
	 * Tested
	 * @param name
	 * @return
	 */
	public static String getRamdomWCSFileName(String name){
		String tmpname =createAutoWCSDir() +"wcs_" +  getRamdomFileName(name);
		return  tmpname ;
	}

	
	public String fixFilenameForWCS(String filename){
		if(filename.startsWith("/"))
			filename = filename.substring(1,filename.length());
		return filename;
	}
	
	/**
	 * 删除指定文件
	 * Tested
	 * @param remoteFilePath
	 * @return MyRet
	 */
	public MyRet delete(String remoteFilePath) {
		MyRet ret = new MyRet();
		if(remoteFilePath==null)
			return ret;
		
		ret.fname = remoteFilePath;
		
		long currentTime = System.currentTimeMillis();
		
		remoteFilePath = fixFilenameForWCS(remoteFilePath);
		
		HttpClientResult httpClientResult = FileManageCommand.delete(
				bucketName, remoteFilePath);
		ret.cost = System.currentTimeMillis() - currentTime;
		ret.error_code = httpClientResult.getStatus();
		if (ret.error_code != 200) {
			ret.error_msg = httpClientResult.getResponse();
		}
		return ret;
	}
	
	/**
	 * Tested
	 * @param fileKey
	 * @param localfile
	 * @return
	 */
	public MyRet download(String fileKey,String localfile){
		MyRet ret = new MyRet();
		long currentTime = System.currentTimeMillis();
		ret.fname = fileKey;
		
		fileKey = fixFilenameForWCS(fileKey);
		
		HttpClientResult httpClientResult = FileDownCommand.download(bucketName, fileKey, localfile);
		ret.cost = System.currentTimeMillis() - currentTime;
		ret.error_code = httpClientResult.getStatus();
		return ret;
	}
	/**
	 * Tested
	 * @param format
	 * @param in
	 * @return
	 */
	public MyRet upload(String format,InputStream in){
		String remoteFilename = getRamdomWCSFileName(format);
		
	    PutPolicy putPolicy = new PutPolicy();
	    putPolicy.setOverwrite(1);
	    putPolicy.setReturnBody(returnBody);
		
		MyRet ret = new MyRet();
		ret.fname = remoteFilename;
		
		long currentTime = System.currentTimeMillis();
		
		remoteFilename = fixFilenameForWCS(remoteFilename);
		
		HttpClientResult httpClientResult = FileUploadCommand
				.upload(bucketName, remoteFilename, remoteFilename,in,
						putPolicy);
		ret.cost = System.currentTimeMillis() - currentTime;
		ret.error_code = httpClientResult.getStatus();
		if (ret.error_code == 200) {
			ret = toMap(ret,
					httpClientResult.getResponse());
		} else {
			ret.error_msg = httpClientResult.getResponse();
		}
		return ret;		
	}
	
	/**
	 * Tested
	 * @param localFilePath
	 * @return
	 */
	public MyRet upload(String localFilePath){
		String remoteFilename = getRamdomWCSFileName(localFilePath);
		return upload(remoteFilename,localFilePath);
	}
	
	/**
	 * 上传本地文件 
	 * Tested
	 * @param remoteFilePath key
	 * @param localFilePath
	 * @return MyRet
	 */
	public MyRet upload(String remoteFilePath, String localFilePath) {
		MyRet ret = new MyRet();
		ret.fname = remoteFilePath;
		
		long currentTime = System.currentTimeMillis();

		remoteFilePath = fixFilenameForWCS(remoteFilePath);
		
		HttpClientResult httpClientResult = FileUploadCommand
				.uploadFileAndReturn(bucketName, remoteFilePath, localFilePath,
						returnBody);
		ret.cost = System.currentTimeMillis() - currentTime;
		ret.error_code = httpClientResult.getStatus();
		if (ret.error_code == 200) {
			ret = toMap(ret,
					httpClientResult.getResponse());
		} else {
			ret.error_msg = httpClientResult.getResponse();
		}
		return ret;
	}
	
	public MyRet toMap(MyRet ret, String callbackBody) {
		//System.out.println(callbackBody);
		String[] arg = StringUtils.split(callbackBody,
				"&");
		for (int i = 0; i < arg.length; i++) {
			String[] keyValue = StringUtils.split(
					arg[i], "=");
			if ("key".equalsIgnoreCase(keyValue[0])) {
				ret.key = keyValue[1];
			} 
			// jsonMap.put(keyValue[0], keyValue[1]);
		}
		return ret;
	}
}
