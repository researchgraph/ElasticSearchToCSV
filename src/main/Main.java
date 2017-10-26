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
	
	private void mergeFiles(String intputDirectoryPath, String outputFilePath, String dataSource, Mode mode){
		Merge merge = new Merge(dataSource, mode);
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
	
//	importSource = GESIS or NIH
	private static void startProcessWithUrl(String url, String outputFilePath, String importSource){
		Main main = new Main();
		
		if(importSource.equals("GESIS")){
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
				main.mergeFiles(cleanedCsvDirPath, outputFilePath, importSource, Mode.NODES);
				System.out.println("Done.");
			}
			else{
				System.out.println("Create relationship csv-file for import");
	//			main.getRelationships(csvDirPath, outputFilePath);
				main.mergeFiles(cleanedCsvDirPath, outputFilePath, importSource, Mode.EDGES);
				System.out.println("Done.");
			}
			System.out.println("Cleanup...");
			main.deleteDirectory(csvDirPath);
			main.deleteDirectory(cleanedCsvDirPath);
			System.out.println("Finished.");
		}
	}
	
	private static void startProcessWithCSV(String csvDirPath, String outputFilePath, String importSource){
		Main main = new Main();

		if(importSource.equals("NIH")){
			main.mergeFiles(csvDirPath, outputFilePath, importSource, Mode.NODES);
		}
		
	}
	
	private static void startProcess(String url_or_csvPath, String outputFilePath, String importSource){
		if(url_or_csvPath.contains("http://"))
			startProcessWithUrl(url_or_csvPath, outputFilePath, importSource);
		else
			startProcessWithCSV(url_or_csvPath, outputFilePath, importSource);
	}
	
	public static void main(String[] args){
//		if(args != null && args.length == 3){
//			startProcess(args[0], args[1], args[3]);
//		}
		String csvDirPath = "C:/Users/du/Projekte/LOD_Research_Graph/nih/importTest";
		String outputFilePath = "C:/Users/du/Projekte/LOD_Research_Graph/nih/nihOutputFiles/nih.csv";
		startProcess(csvDirPath, outputFilePath, "NIH");
	}
}
