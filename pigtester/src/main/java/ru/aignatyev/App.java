package ru.aignatyev;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;

import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecException;

public class App 
{
	//TODO parameterize filenames and remove from script
	//TODO names
	//TODO load resources
	private static final String script = "/src/main/resources/script.pig";
    private static final String input = "/src/main/resources/input.txt";
    private static String output = "/src/main/resources/output.txt";
    private static String RESULT_FOLDER = "/target/result";

	public static void main( String[] args )
    {
        PigServer pigServer = null;
        String mode = "local";

        Map<String,String> params = null;

        List<String> paramFiles = null;

        try {
            pigServer = new PigServer(mode);
            pigServer.setBatchOn();
            pigServer.debugOn();
            pigServer.registerScript(script);
            pigServer.executeBatch();

            testScriptResult();
        } catch (Exception e) {
            throw new RuntimeException(e);
        /*} finally {
            if(pigServer != null){
                pigServer.shutdown();
            }*/
        }
    }

	private static String readFileToString(String fileName) throws IOException {
		InputStream fis = new FileInputStream(fileName);
		int ch;
		StringBuilder builder = new StringBuilder();
		while((ch = fis.read()) != -1) { 
			builder.append((char) ch); 
		}
		fis.close();
		return builder.toString();
	}

	private static void testScriptResult() throws IOException {
        //getting ONLY FIRST output path from the script
        String scriptText = readFileToString(script);
        String pattern = "store .* into ['\"](.*)['\"]";
        RESULT_FOLDER = scriptText.toLowerCase().replaceFirst(pattern, "$0");

        String expected = readFileToString(output);
		String actual = readFileToString(RESULT_FOLDER + "/part-m-00000"); //TODO hardcoded output fileName
		System.out.println("Pig script (" + script + ")  tested.");
		System.out.println("Test passed: " + expected.equals(actual));
	}
}