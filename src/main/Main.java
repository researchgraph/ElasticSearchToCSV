package main;
import java.io.File;

import json2csv.CSVCleaner;
import json2csv.Json2Csv;
import json2csv.Merge;
import json2csv.Merge.Mode;

public class Main {
	
	public Main(){
		
	}
	
	private void loadAndTransformData(String url, String outputFilePath){
		Json2Csv jc = new Json2Csv();
		jc.doTransformation(url, outputFilePath);
	}
	
	private void cleanData(String inputDirectoryPath, String outputDirectoryPath){
		CSVCleaner cleaner = new CSVCleaner();
		cleaner.cleanCsvFiles(inputDirectoryPath, outputDirectoryPath);
	}
	
	private void mergeFiles(String intputDirectoryPath, String outputFilePath, Mode mode){
		Merge merge = new Merge(mode);
		merge.mergeCsvFiles(intputDirectoryPath, outputFilePath);
	}
	
	private void deleteDirectory(String path){
		File file = new File(path);
		
		if(file.exists()){
			if(file.isDirectory()){
				for(File subFile: file.listFiles()){
					subFile.delete();
				}
			}
			file.delete();
		}			
	}
	
	private static void startProcess(String url, String outputFilePath){
		Main main = new Main();
		String csvDirPath = outputFilePath + "_/";
		String cleanedCsvDirPath = outputFilePath + "_cleaned/";
		
		System.out.println("Loading and transforming data...");
		main.loadAndTransformData(url, csvDirPath + (new File(outputFilePath)).getName());
		System.out.println("Done.");
		
		System.out.println("Preparing data for import...");
		main.cleanData(csvDirPath, cleanedCsvDirPath);
		System.out.println("Done.");
		
		if(!url.contains("EntityLink")){			
			System.out.println("Create node csv-file for import...");
			main.mergeFiles(cleanedCsvDirPath, outputFilePath, Mode.entity);
			System.out.println("Done.");
		}
		else{
			System.out.println("Create relationship csv-file for import");
//			main.getRelationships(csvDirPath, outputFilePath);
			main.mergeFiles(cleanedCsvDirPath, outputFilePath, Mode.entityLink);
			System.out.println("Done.");
		}
		System.out.println("Cleanup...");
		main.deleteDirectory(csvDirPath);
		main.deleteDirectory(cleanedCsvDirPath);
		System.out.println("Finished.");
	}
	
	public static void main(String[] args){
		if(args != null && args.length == 2){
			startProcess(args[0], args[1]);
		}
	}
}
