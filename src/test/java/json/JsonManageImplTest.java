package json;

import static org.junit.Assert.assertSame;

import org.junit.Test;

import constants.JsonConstants;

public class JsonManageImplTest {

	@Test
	public void testExistFile() {		
		String filePath = System.getProperty("user.dir") + JsonConstants.MOVIES_FILE_PATH + "\\" + JsonConstants.MOVIES_FILE_NAME;
		JsonManageImpl jsonManageImpl = new JsonManageImpl();
		assertSame(true,  jsonManageImpl.existFile(filePath));
	}

	@Test
	public void testCanReadFile() {
		String filePath = System.getProperty("user.dir") + JsonConstants.MOVIES_FILE_PATH + "\\" + JsonConstants.MOVIES_FILE_NAME;
		JsonManageImpl jsonManageImpl = new JsonManageImpl();
		assertSame(true,  jsonManageImpl.canReadFile(filePath));
	}

}
