package json2csv;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.TreeSet;

import org.apache.commons.io.FileUtils;

import tools.Tools;

public class Merge {
	private String csvSeparator = ",";
	public Merge(){
		
	}
	
//	### merge csv files #########################################
	public void mergeCsvFiles(String csvDirPath, String csvOutputPath){
		TreeSet<String> columns = getAllColumnsFromHeaders(csvDirPath);
		HashMap<String, Integer> columnsWithIndexes = defineColumnIndexes(columns);
		
		File[] files = Tools.getFilesFromDir(csvDirPath);
		String content, header;
		String[] fileColumns;
		String[] rows;
		
		String newHeader = Tools.getArrayAsString(columns, csvSeparator);
		newHeader = prepareHeader(newHeader);
		String mergedFilesContent;
		
		File outFile = new File(csvOutputPath);
		try {
			FileUtils.writeStringToFile(outFile, newHeader + "\n", "UTF-8");
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		
		
		for(File inFile: files){
			try {
				content = FileUtils.readFileToString(inFile, "UTF-8");
				
				header = content.substring(0, content.indexOf("\n"));
				fileColumns = header.split(csvSeparator);
				
				content = content.substring(content.indexOf("\n") +1);
				rows = content.split("\n");
				
				mergedFilesContent = "";
				for(String row: rows){
					mergedFilesContent += getOrderedRow(row, fileColumns, columnsWithIndexes) + "\n";
				}
				
				try {
					FileUtils.writeStringToFile(outFile, mergedFilesContent, "UTF-8", true);
				} catch (IOException e1) {
					e1.printStackTrace();
				}
				
				
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		
	}
	
	public String prepareHeader(String header){
		header = header.replace("gwsId", "gwsId:ID");
		header = header.replace("_source.", "");
		
		return header;
	}
	
	
	public String getOrderedRow(String unorderedRow, String[] fileHeader, HashMap<String, Integer> columnsWithIndexes){
		CSVCleaner cleaner = new CSVCleaner();
		String[] orderedElements = Tools.getInitializedStringArray(columnsWithIndexes.size());
		
		ArrayList<String> rowElements = cleaner.getElements(unorderedRow);
		
		for(int i = 0; i < fileHeader.length; i++){
			orderedElements[columnsWithIndexes.get(fileHeader[i].trim())] = rowElements.get(i).trim();
		}
		
		
		return Tools.getArrayAsString(orderedElements, csvSeparator);
	}
	
	
	
	public HashMap<String, Integer> defineColumnIndexes(TreeSet<String> columns){
		HashMap<String, Integer> columnsWithIndexes = new HashMap<>();
		
		int index = 0;
		for(String column: columns){
			columnsWithIndexes.put(column, index++);
		}		
		
		return columnsWithIndexes;
	}
	
	private TreeSet<String> getAllColumnsFromHeaders(String csvDirPath){
		TreeSet<String> columnsTotal = new TreeSet<>();
		File[] files = Tools.getFilesFromDir(csvDirPath);
		String content, header;
		String[] columns;
		
		for(File inFile: files){
			try {
				content = FileUtils.readFileToString(inFile, "UTF-8");

				header = content.substring(0,  content.indexOf("\n"));
				columns = header.split(csvSeparator);
				
				for(String column: columns){
					columnsTotal.add(column.trim());
				}
				
			} catch (IOException e) {
				e.printStackTrace();
			}
			
		}
		
		return columnsTotal;
	}
	
	
	
	public void mergeFiles(File[] sourceFiles, String resultFilePath) throws IOException{
		File resultFile = new File(resultFilePath);
		if(!resultFile.exists()){
			resultFile.createNewFile();
		}

		System.out.println("files: " + sourceFiles.length);
		String fileContent;
		
		for(File file: sourceFiles){
			fileContent = FileUtils.readFileToString(file, "UTF-8");
			FileUtils.writeStringToFile(resultFile, fileContent, "UTF-8", true);
		}
	}
}
