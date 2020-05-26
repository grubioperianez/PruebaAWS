package json;

import java.io.File;
import java.io.IOException;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;

import logs.LogWriter;

public class JsonManageImpl implements JsonManage {

	public JsonManageImpl() {
		// TODO Auto-generated constructor stub
	}


	@Override
	public boolean existFile(String pathToFile) {
		
		boolean fileExists = true;
		
		File fichero = new File(pathToFile);

		if (fichero.exists())
		  System.out.println("El fichero " + pathToFile + " existe");
		else
			fileExists = false;

		return fileExists;
	}

	@Override
	public boolean canReadFile(String pathToFile) {
		
		boolean canReadFile = true;

		try {
			JsonParser parser = new JsonFactory().createParser(new File(pathToFile));
			LogWriter.writeLog("El fichero puede ser leido...");
			parser.close();			
		} catch (JsonParseException e) {
			canReadFile = false;
			LogWriter.writeLog("ERROR: " + e);
		} catch (IOException e) {
			canReadFile = false;
			LogWriter.writeLog("ERROR: " + e);
		}

		return canReadFile;
	}

}
