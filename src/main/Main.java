package main;

import json2csv.Json2Csv;

public class Main {
	
	public Main(){}
	
	private void loadAndTransformData(String url, String outputFilePath, String fileNamePrefix){
		Json2Csv jc = new Json2Csv();
		jc.doTransformation(url, outputFilePath, fileNamePrefix);
	}
	
	
	private static void startProcess(String url, String outputDirectoryPath, String fileNamePrefix){
		Main main = new Main();	
		if(!outputDirectoryPath.endsWith("/"))
			outputDirectoryPath += "/";
		
		System.out.println("Loading and transforming data...");
		main.loadAndTransformData(url, outputDirectoryPath, fileNamePrefix);
		System.out.println("Done.");
	
	}
	
	
	public static void main(String[] args){
		if(args != null){
			if(args.length == 2)
				startProcess(args[0], args[1], "");
			if(args.length == 3)
				startProcess(args[0], args[1], args[2]);
		}
	}
}
