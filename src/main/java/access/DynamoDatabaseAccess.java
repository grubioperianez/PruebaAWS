package access;

public interface DynamoDatabaseAccess {
	
	/**
	 * 
	 * @return boolean
	 * @throws Exception
	 */
	abstract boolean  initConnexion();
	
	
	

	/**
	 * 
	 * @param tableName
	 * @return boolean
	 * @throws Exception
	 */
	abstract boolean createTable(String tableName);
	
	
	/**
	 * 
	 * @param tableName
	 * @return String
	 * @throws Exception
	 */
	abstract String describeTable(String tableName);
	
	
	/**
	 * @param String tableName
	 * @return boolean
	 * @throws Exception
	 */
	abstract boolean insertItemIntoTable(String tableName) throws Exception;

}
