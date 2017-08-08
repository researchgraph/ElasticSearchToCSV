package tools;

import java.io.File;
import java.io.IOException;
import java.util.Set;

import org.apache.commons.io.FileUtils;

public class Tools {	
	
	public static File[] getFilesFromDir(String dirPath){
		File csvDir = new File(dirPath);
		return csvDir.listFiles();
	}
	
	public static String[] getInitializedStringArray(int length){
		String[] stringArray = new String[length];
		
		for(int i = 0; i < length; i++){
			stringArray[i] = "";
		}
		
		return stringArray;
	}
	
	public static String getArrayAsString(Set<String> values, String valueSeparator){
		String[] valueArray = new String[values.size()];
		values.toArray(valueArray);
		
		return getArrayAsString(valueArray, valueSeparator);
	}
	
	public static String getArrayAsString(String[] values, String valueSeparator){
		String valueString = "";
		
		for(String value: values){
			valueString += value + valueSeparator;
		}
		
		if(!valueSeparator.isEmpty())
			valueString = valueString.substring(0, valueString.lastIndexOf(valueSeparator));
		
		return valueString;
	}
	
	public static void printListToFile(Set<String> list, String valueSeparator, String filePath) throws IOException{
		String fileContent = getArrayAsString(list, valueSeparator);
		
		FileUtils.writeStringToFile(new File(filePath), fileContent, "UTF-8");
	}

}
