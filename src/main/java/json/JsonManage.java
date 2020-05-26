package json;

public interface JsonManage {
	

	/**
	 * 
	 * @param pathToFile
	 * @return boolean
	 */
	abstract boolean existFile(String pathToFile);
	
	
	/**
	 * 
	 * @param pathToFile
	 * @return boolean
	 */
	abstract boolean canReadFile(String pathToFile);
}
