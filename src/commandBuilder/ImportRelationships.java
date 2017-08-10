package commandBuilder;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.commons.io.FileUtils;

import json2csv.CSVCleaner;
import tools.Tools;

public class ImportRelationships {
	private final String csvSeparator = ",";
	private final String relationshipType = "relatedTo";
	
	public ImportRelationships(){
		
	}
	
//	### create relationship csv ################################
	private int numColumnsTotal = 0;
	public void createRelationshipCsv(String inPath, String outPath){
		File relationshipCsvFile = new File(outPath);
		
		String csvHeader = ":START_ID,:END_ID,:TYPE";
		String csvContent = "", relationshipContent;
		RelationShip relationship;
		
		File[] csvFiles = Tools.getFilesFromDir(inPath);
		
		try {
			FileUtils.writeStringToFile(relationshipCsvFile, csvHeader, "UTF-8");
			for(File csvFile: csvFiles){
				csvContent = FileUtils.readFileToString(csvFile, "UTF-8");
				
				String header = csvContent.substring(0,  csvContent.indexOf("\n"));
				setColumnIndexes(header);
				numColumnsTotal = getNumColumns(header);
				
				csvContent = csvContent.substring(csvContent.indexOf("\n") +1);				
				String[] lines = csvContent.split("\n");				
				
				relationshipContent = "";
				for(String line: lines){
					relationship = getRelationShipFromLine(line);
					relationshipContent += "\n" + relationship.fromGwsId + "," + relationship.toGwsId + "," + relationshipType;					
				}
				FileUtils.writeStringToFile(relationshipCsvFile, relationshipContent, "UTF-8", true);
			}
		} catch (IOException e1) {
			e1.printStackTrace();
		}		
	}	
	
	
	
//	#### get Relationship #####################################		
	private RelationShip getRelationShipFromLine(String line){
		RelationShip relationship = new RelationShip();
		
		CSVCleaner cleaner = new CSVCleaner();
		cleaner.setCsvSeparator(csvSeparator);
		ArrayList<String> csvElements = cleaner.getElements(line);
		
		if(csvElements.size() != numColumnsTotal){
			try {
				throw new Exception();
			} catch (Exception e) {
				System.out.println("Illegal number of columns:" + numColumnsTotal + "->" + csvElements.size() + "\n" + line);
				e.printStackTrace();
			}
		}
		else{
			relationship.fromGwsId = csvElements.get(fromGwsIdIndex);
			relationship.toGwsId = csvElements.get(toGwsIdIndex);
		}		
		
		return relationship;
	}
	
	private int getNumColumns(String header){
		return header.split(csvSeparator).length;
	}
	
	
//	### Index ##############################################################
	
	public int fromGwsIdIndex = 0;
	public int toGwsIdIndex = 0;
	public int fromTypeIndex = 0;
	public int toTypeIndex = 0;
	
	public void setColumnIndexes(String header){
		fromGwsIdIndex = getFromGwsIdIndex(header);
		toGwsIdIndex = getToGwsIdIndex(header);
		fromTypeIndex = getFromTypeIndex(header);
		toTypeIndex = getToTypeIndex(header);
	}
	
	private int getFromGwsIdIndex(String header){
		return getIndex(header, "_source.gws_fromID");
	}
	
	private int getToGwsIdIndex(String header){
		return getIndex(header, "_source.gws_toID");
	}
	
	private int getFromTypeIndex(String header){
		return getIndex(header, "_source.gws_fromType");
	}
	
	private int getToTypeIndex(String header){
		return getIndex(header, "_source.gws_toType");
	}
	
	private int getIndex(String header, String columnName){
		int index = 0;
		String[] columns = header.split(csvSeparator);
		
		for(int i = 0; i < columns.length; i++){
			if(columns[i].equals(columnName)){
				index = i;
				break;
			}
		}
		
		return index;
	}
//	########################################################################	
	
	
	private class RelationShip{
		public String fromGwsId = "";
		public String toGwsId = "";
	}
}
