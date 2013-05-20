package ru.aignatyev;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;

import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecException;

public class App 
{
	//TODO parameterize filenames and remove from script
	//TODO names
	//TODO load resources
	private static final String script = "/src/main/resources/script.pig";
    private static final String input = "/src/main/resources/input.txt";
    private static final String output = "/src/main/resources/output.txt";
    private static final String RESULT_FOLDER = "/target/result";
    
	public static void main( String[] args )
    { 
		/*script = System.getProperty("script");
		input = System.getProperty("input");
		output = System.getProperty("output");*/
			
        try {
        	PigServer pigServer = new PigServer("local");
			pigServer.registerQuery("A = load '" + input + "' using PigStorage(':');");
			
//			pigServer.registerQuery("B = foreach A generate $0 as id;");
			pigServer.registerQuery(readFileToString(script));
			
			pigServer.store("B", RESULT_FOLDER);
        	
        	/*Map<String, String> params = new HashMap<String, String>();
        	params.put("data", DATA);
        	params.put("result", RESULT_FOLDER);
        	pigServer.registerScript(SCRIPT, params);
        	pigServer.openIterator("B");*/
			
			testScriptResult(); 
		} catch (ExecException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }

	private static String readFileToString(String fileName) throws FileNotFoundException,
			IOException {
		InputStream fis = new FileInputStream(fileName);
		int ch;
		StringBuilder builder = new StringBuilder();
		while((ch = fis.read()) != -1) { 
			builder.append((char) ch); 
		}
		fis.close();
		return builder.toString();
	}

	private static void testScriptResult() throws FileNotFoundException, IOException {
		String expected = readFileToString(output);
		String actual = readFileToString(RESULT_FOLDER + "/part-m-00000"); //TODO hardcoded output fileName
		System.out.println("Pig script (" + script + ")  tested.");
		System.out.println("Test passed: " + expected.equals(actual));
	}
}