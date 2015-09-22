package cn.qtone.FileSystem;

import com.qiniu.storage.BucketManager;
import com.qiniu.storage.UploadManager;
import com.qiniu.util.Auth;
import com.qiniu.util.StringMap;

public class Qiniu  extends YZTFS {
	private static String ACCESS_KEY="PbF1_K88t7WEkhRJtyuegUTg-YicnKalu4p6kHVi";
	private static String SECRET_KEY="kQkwX102c6pTIycQhJlJbDbu3sKKqzCNpHALH9Tj";
	private static Auth auth = Auth.create(ACCESS_KEY, SECRET_KEY);
	private static UploadManager uploadManager = new UploadManager();
	private static BucketManager bucketManager = null;
	private static final String bucket = "album";	
	
	public static BucketManager getBucketManager() {
		if(bucketManager==null)
			new BucketManager(auth);
		return bucketManager;
	}

	
	public static Auth getAuth() {
		return auth;
	}

	public static UploadManager getUploadManager() {
		return uploadManager;
	}
	
	public String getUpToken(){
		return getUpToken(null);
	}
	
	public String getUpToken(String key ){
	    return auth.uploadToken(bucket, key, 3600, new StringMap()
	            .putNotEmpty("returnBody", "{\"key\": $(key), \"hash\": $(etag),\"fsize\":$(fsize),\"uuid\":$(uuid), \"width\": $(imageInfo.width), \"height\": $(imageInfo.height)}"));
	}
	
	public static String createAutoHadoopDir(){
		return createAutoDirs();
	}


	public static String getRamdomHadoopFileName(String name){
		String tmpname = getRamdomFileName(name);
		return "qiniu" + "_" + tmpname ;
	}
}
