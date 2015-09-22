package cn.qtone.FileSystem;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.fs.FileSystem;


public class Hadoop extends YZTFS{


	private static final String[][] shardservers = new String[][]{
		{"0","hdfs://namenode-0-vip:8020"},
		{"1","hdfs://namenode-1-vip:8020"}
	};

	public static String[][] getShards(){
		return shardservers;
	}

	static final Map<Integer, FileSystem> fsList = new HashMap<Integer, FileSystem>();
	
	public static void addFS(int shard,FileSystem fs){
		fsList.put(shard, fs);
	}
	

	/**
	 * 
	 * @param shards
	 * @return
	 * @throws NullPointerException
	 */
	
	protected static FileSystem getFs(int shards) throws NullPointerException {
		FileSystem fs = fsList.get(shards);
		if (fs == null)
			throw new NullPointerException("Hadoop FileSystem Config ERROR.");
		return fs;
	}

	/**
	 * 
	 * @param shards
	 * @return
	 * @throws NullPointerException
	 */
	
	protected static FileSystem getFs() throws NullPointerException {
		return getFs(0);
	}

	/**
	 * cale filename shardnumber
	 * @param filename
	 * @return
	 */
	public static int getShard(String filename){
		int pos = filename.indexOf("shark_");
		if(pos>-1){
			String shard_number = filename.substring(pos+6,pos+7);
			return Long.valueOf(shard_number).intValue();
		}else{
			return 0;
		}
	}
	
	/**
	 * Hash filename to cale the shardNumber
	 * @param filename
	 * @return
	 */
	private static int caleFileShardNumber(String filename){
		int pos = filename.indexOf("shark_");
		if(pos>-1){
			String shard_number = filename.substring(pos+6,pos+7);
			return Long.valueOf(shard_number).intValue();
		}else{
			int hc = filename.hashCode();
			long hcode =Math.round(hc*0.1)*10;
			long a_mod = (hcode<hc?hc-hcode:hcode-hc);
			//System.out.println(filename+" : "+a_mod%shardservers.length);
			return new Long(a_mod%shardservers.length).intValue();
		}
	}

	public static String createAutoHadoopDir(){
		return createAutoDirs();
	}


	public static String getRamdomHadoopFileName(String name){
		String tmpname = getRamdomFileName(name);
		return "shark_" + caleFileShardNumber(tmpname)+"_" + tmpname ;
	}

}
