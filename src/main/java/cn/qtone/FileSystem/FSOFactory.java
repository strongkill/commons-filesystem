package cn.qtone.FileSystem;


public class FSOFactory {
/**
 * 使用参数指定存储系统，构造FSO对象
 * Support file system include : HADOOP WCS QiniuFS
 * @param fsotype
 * @return
 */
	public static IFSO buildFSO(String fsotype){
		IFSO fso = null;
		if(YZTFS.Hadoopfsotype.equalsIgnoreCase(fsotype))
			fso = new HadoopFSO();
		if(YZTFS.WCSfsotype.equalsIgnoreCase(fsotype))
			fso = new WcsFSO();
		if(YZTFS.Qiniufsotype.equalsIgnoreCase(fsotype))
			fso = new QiniuFSO();
		return fso;
	}
	
	
	/**
	 * 使用默认存储系统构造FSO对象
	 * @return
	 * 
	 */
	public static IFSO buildFSO(){
		return buildFSO(YZTFS.DEFAULTFSTYPE);
	}	
	/**
	 * 通过文件名称来获取文件所在的存储系统，并构建对象
	 * 
	 * @param filename
	 * @return
	 */
	public static IFSO buildFSOWitFilename(String filename){
		return buildFSO(filesavearea(filename));
	}

	
	/**
	 * 通过文件名，获取存储文件的系统名称
	 * @param filename
	 * @return
	 */
	private static String filesavearea(String filename){
		if(filename.indexOf("shark_")>-1)
			return YZTFS.Hadoopfsotype;
		if(filename.indexOf("qinuiu_")>-1)
			return YZTFS.Qiniufsotype;
		if(filename.indexOf("wcs_")>-1)
			return YZTFS.WCSfsotype;		
		return YZTFS.Hadoopfsotype;
	}
}
