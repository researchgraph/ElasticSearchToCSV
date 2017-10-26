package json2csv;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.commons.io.FileUtils;

public class CSVCleaner {
	private String csvSeperator = ",";
	private int numColumns = 0;
	
	public CSVCleaner(){
		
	}
	
	public void cleanCsvFiles(String csvDirPath, String outpath){
		File csvDir = new File(csvDirPath);
		File[] files = csvDir.listFiles();		
		
		for(File file: files){
			cleanCsvFile(file, new File(outpath + file.getName()));
		}		
	}
	
	public void cleanCsvFile(File inFile, File outFile){
		try {
			String content, header;
			content = FileUtils.readFileToString(inFile, "UTF-8");
			
			header = content.substring(0,  content.indexOf("\n"));
			numColumns = header.split(csvSeperator).length;
			content = content.substring(content.indexOf("\n") +1);
			
			String[] lines = content.split("\n");
			content = "";
			for(String line: lines){
				content += cleanLine(line) + "\n";
			}
			
			FileUtils.writeStringToFile(outFile, header + "\n" + content, "UTF-8");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public String cleanLine(String line){
		ArrayList<String> elements = getElements(line);
		try {
			if(elements.size() != this.numColumns)
				throw new Exception();
		} catch (Exception e) {
			System.out.println("Illegal number of columns:" + numColumns + "->" + elements.size() + "\n" + line);
			e.printStackTrace();
		}		
		
		line = "";
		for(String element: elements){
			line += cleanElement(element) + csvSeperator;
		}
		
//		line = listToLine(elements);		
		
		return line.substring(0, line.lastIndexOf(csvSeperator));
	}
	
	public void setCsvSeparator(String csvSeparator){
		this.csvSeperator = csvSeparator;
	}
	
	private String cleanElement(String element){
		
		if(element.contains("\"")){
			boolean addQuotes = false;
			
			if(element.startsWith("\"") && element.endsWith("\""))
				addQuotes = true;			
			
			element = element.replace("\"", "");
			
			if(addQuotes)
				element = "\"" + element + "\"";				
		}		
		
		return element;		
	}
	
	
	public ArrayList<String> getElements(String line){
		ArrayList<String> elements = new ArrayList<>();
		
		String[] separation = line.split(csvSeperator, 100000);
		String prefix = "";
		for(String element: separation){	
			element = element.trim();
			if(prefix.isEmpty()){
				if(element.startsWith("\"")){
					if(element.endsWith("\"") && element.length() > 1)
						elements.add(element);
					else
						prefix = element;
				}
				else
					elements.add(element);
			}
			else{
				if(element.endsWith("\"")){
					elements.add(prefix + csvSeperator + element);
					prefix = "";
				}	
				else
					prefix += csvSeperator + element;
			}
		}
		return elements;
	}
}
