package database;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class DynamoDatabaseAccessImplTest {
	
	@Test
	public void initConnexion() {
		DynamoDatabaseAccessImpl obj = new DynamoDatabaseAccessImpl();
		assertEquals(true, obj.initConnexion());
	}

}
