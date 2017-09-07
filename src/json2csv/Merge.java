package json2csv;
import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAccessor;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Locale;
import java.util.TimeZone;
import java.util.TreeSet;

import org.apache.commons.io.FileUtils;

import tools.Tools;

public class Merge {
	private String csvSeparator = ",";
	
	private HashMap<String, HashMap<String, Integer>> columnIndexMap;
	private HashMap<String, String> headerMap;
	private String currentDateAndTime;
	
	public Merge(){
		setCurrentDateAndTime();
		setColumnIndicesAndHeaders();
	}
	
	private void setCurrentDateAndTime(){
		TimeZone timeZone = TimeZone.getTimeZone("UTC");
		Calendar calendar = Calendar.getInstance(timeZone);

		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("EEE MMM dd hh:mm:ss 'UTC' yyyy", Locale.US);
		simpleDateFormat.setTimeZone(timeZone);
		
		String time = simpleDateFormat.format(calendar.getTime());
		
		this.currentDateAndTime = time;
	}
	
	
//	### headers and mappings #######################################
	
	public enum EntityTypes{
		
		citedData("citedData"),
		dataset("dataset"),
		institution("institution"),
		instrument("instrument"),
		project("project"),
		publication("publication");
		
		private final String name;
		
		private EntityTypes(String s){
			this.name = s;
		}
		
		public String toString(){
			return this.name();
		}
	}
	
	private void setColumnIndicesAndHeaders(){
		TreeSet<String> citedDataColumns = getCitedDataColumns();
		TreeSet<String> datasetColumns = getDatasetColumns();
		TreeSet<String> institutionColumns = getInstitutionColumns();
		TreeSet<String> instrumentColumns = getInstrumentColumns();
		TreeSet<String> projectColumns = getProjectColumns();
		TreeSet<String> publicationColumns = getPublicationColumns();
		
		columnIndexMap = new HashMap<>();
		columnIndexMap.put(EntityTypes.citedData.toString(), defineEntityIndexes(citedDataColumns));
		columnIndexMap.put(EntityTypes.dataset.toString(), defineEntityIndexes(datasetColumns));
		columnIndexMap.put(EntityTypes.institution.toString(), defineEntityIndexes(institutionColumns));
		columnIndexMap.put(EntityTypes.instrument.toString(), defineEntityIndexes(instrumentColumns));
		columnIndexMap.put(EntityTypes.project.toString(), defineEntityIndexes(projectColumns));
		columnIndexMap.put(EntityTypes.publication.toString(), defineEntityIndexes(publicationColumns));
		
		headerMap = new HashMap<>();
		headerMap.put(EntityTypes.citedData.toString(), createHeader(citedDataColumns, EntityTypes.citedData));
		headerMap.put(EntityTypes.dataset.toString(), createHeader(datasetColumns, EntityTypes.dataset));
		headerMap.put(EntityTypes.institution.toString(), createHeader(institutionColumns, EntityTypes.institution));
		headerMap.put(EntityTypes.instrument.toString(), createHeader(instrumentColumns, EntityTypes.instrument));
		headerMap.put(EntityTypes.project.toString(), createHeader(projectColumns, EntityTypes.project));
		headerMap.put(EntityTypes.publication.toString(), createHeader(publicationColumns, EntityTypes.publication));
	}	
	
	
//	### columns ###
	
	private TreeSet<String> getCitedDataColumns(){
		TreeSet<String> columns = new TreeSet<>();
//		columns.add("_id");
		columns.add("_index");
//		columns.add("_score");
		columns.add("_source.entityProvenance");
		columns.add("_source.entityReliability");
		columns.add("_source.entityType");
		columns.add("_source.entityView");
		columns.add("_source.freeKeywords");
		columns.add("_source.gwsId");
		columns.add("_source.name");
		columns.add("_source.numericInfo");
		columns.add("_source.year");
		columns.add("_type");
		
		columns.add("type");
		
		return columns;
	}
	private TreeSet<String> getDatasetColumns(){
		TreeSet<String> columns = new TreeSet<>();
//		columns.add("_id");
		columns.add("_index");
//		columns.add("_score");
//		columns.add("_source.abstractText");
		columns.add("_source.authors");
		columns.add("_source.doi");
		columns.add("_source.entityProvenance");
		columns.add("_source.entityReliability");
		columns.add("_source.entityType");
		columns.add("_source.entityView");
		columns.add("_source.gwsId");
		columns.add("_source.identifiers");
		columns.add("_source.name");
		columns.add("_source.numericInfo");
		columns.add("_source.publisher");
		columns.add("_source.year");
		columns.add("_type");
		
//		Research Graph schema
		columns.add("key");		
		columns.add("source");	
		columns.add("last_updated");	
		columns.add("licence");	
		columns.add("megabyte");
		columns.add("type");
		
		return columns;
	}
	
	private TreeSet<String> getInstitutionColumns(){
		TreeSet<String> columns = new TreeSet<>();
//		columns.add("_id");
		columns.add("_index");
//		columns.add("_score");
		columns.add("_source.entityProvenance");
		columns.add("_source.entityReliability");
		columns.add("_source.entityType");
		columns.add("_source.entityView");
		columns.add("_source.gwsId");
		columns.add("_source.identifiers");
		columns.add("_source.name");
		columns.add("_type");		

		columns.add("type");
		
		return columns;
	}
	
	private TreeSet<String> getInstrumentColumns(){
		TreeSet<String> columns = new TreeSet<>();
//		columns.add("_id");
		columns.add("_index");
//		columns.add("_score");
		columns.add("_source.doi");
		columns.add("_source.entityProvenance");
		columns.add("_source.entityReliability");
		columns.add("_source.entityType");
		columns.add("_source.entityView");
		columns.add("_source.gwsId");
		columns.add("_source.identifiers");
		columns.add("_source.name");
		columns.add("_source.url");
		columns.add("_type");		

		columns.add("type");
		
		return columns;
	}
	
	private TreeSet<String> getProjectColumns(){
		TreeSet<String> columns = new TreeSet<>();
//		columns.add("_id");
		columns.add("_index");
//		columns.add("_score");
//		columns.add("_source.abstractText");
		columns.add("_source.authors");
		columns.add("_source.entityProvenance");
		columns.add("_source.entityReliability");
		columns.add("_source.entityType");
		columns.add("_source.entityView");
		columns.add("_source.gwsId");
		columns.add("_source.identifiers");
		columns.add("_source.name");
		columns.add("_source.numericInfo");
		columns.add("_source.spatial");
		columns.add("_source.url");
		columns.add("_source.year");
		columns.add("_type");
		
//		Research Graph schema
		columns.add("key");		
		columns.add("source");	
		columns.add("last_updated");	
		columns.add("purl");	
		columns.add("participant_list");
		columns.add("funder");		
		columns.add("end_year");
		columns.add("type");
		
		return columns;
	}
	
	private TreeSet<String> getPublicationColumns(){
		TreeSet<String> columns = new TreeSet<>();
//		columns.add("_id");
		columns.add("_index");
//		columns.add("_score");
		columns.add("_source.authors");
		columns.add("_source.collectionTitle");
		columns.add("_source.doi");
		columns.add("_source.editors");
		columns.add("_source.entityProvenance");
		columns.add("_source.entityReliability");
		columns.add("_source.entityType");
		columns.add("_source.entityView");
		columns.add("_source.gwsId");
		columns.add("_source.identifiers");
		columns.add("_source.isbn");
		columns.add("_source.issn");
		columns.add("_source.journalTitle");
		columns.add("_source.language");
		columns.add("_source.location");
		columns.add("_source.month");
		columns.add("_source.name");
		columns.add("_source.number");
		columns.add("_source.pages");
		columns.add("_source.publicationStatus");
		columns.add("_source.publicationType");
		columns.add("_source.publisher");
		columns.add("_source.seriesTitle");
		columns.add("_source.url");
		columns.add("_source.volume");
		columns.add("_source.year");
		columns.add("_type");
		
//		Research Graph schema
		columns.add("key");		
		columns.add("source");	
		columns.add("last_updated");	
		columns.add("scopus_eid");	
		columns.add("type");
		
		return columns;
	}
	
//	### indices ###
	
	private HashMap<String, Integer> defineEntityIndexes(TreeSet<String> columns){
		return defineColumnIndexes(columns);
	}
	
	private HashMap<String, Integer> defineColumnIndexes(TreeSet<String> columns){
		HashMap<String, Integer> columnsWithIndexes = new HashMap<>();
		
		int index = 0;
		for(String column: columns){
			columnsWithIndexes.put(column, index++);
		}		
		
		return columnsWithIndexes;
	}
	
//	### headers ####
	
	private String createHeader(TreeSet<String> columns, EntityTypes entityType){
		String header = Tools.getArrayAsString(columns, csvSeparator);
		return prepareHeader(header, entityType);
	}
	
	private String prepareHeader(String header, EntityTypes entityType){
		header = header.replace("_source.", "");		
		header = doHeaderColumnNameMapping(header, entityType);
		
		return header;
	}
	
//	### mapping ###
	public String doHeaderColumnNameMapping(String header, EntityTypes entityType){
//		header = header.replace("_id", "");
		header = header.replace("_index", "gesis_index");
//		header = header.replace("_score", "");
//		header = header.replace("abstractText", "");
		header = header.replace("_type", "gesis_type");
		header = header.replace("alternativeNames", "gesis_alternative_names");
		header = header.replace("authors", "authors_list");/*in rg-schema*/
		header = header.replace("classification", "gesis_classification");
		header = header.replace("collectionTitle", "gesis_collection_title");
		header = header.replace("doi", "doi");/*in rg-schema*/
		header = header.replace("editors", "gesis_editors");
		header = header.replace("entityProvenance", "gesis_entity_provenance");
		header = header.replace("entityReliability", "gesis_entity_reliability");
		header = header.replace("entityType", "gesis_entity_type");
		header = header.replace("entityView", "gesis_entity_view");
		header = header.replace("freeKeywords", "gesis_free_keywords");
		header = header.replace("gwsId", "local_id:ID");/*in rg-schema*/
		header = header.replace("identifiers", "gesis_identifiers");
		header = header.replace("isbn", "gesis_isbn");
		header = header.replace("issn", "gesis_issn");
		header = header.replace("journalTitle", "gesis_journal_title");
		header = header.replace("language", "gesis_language");
		header = header.replace("location", "gesis_location");
		header = header.replace("methodKeywords", "gesis_method_keywords");
		header = header.replace("month", "gesis_month");
		header = header.replace("name", "title");/*in rg-schema*/
		header = header.replace("number", "gesis_number");
		header = header.replace("numericInfo", "gesis_numeric_info");
		header = header.replace("pages", "gesis_pages");
		header = header.replace("publicationStatus", "gesis_publication_status");
		header = header.replace("publicationType", "gesis_publication_type");
		header = header.replace("publisher", "gesis_publisher");
		header = header.replace("seriesTitle", "gesis_series_title");
		header = header.replace("spatial", "gesis_spatial");
		header = header.replace("subjects", "gesis_subjects");
		header = header.replace("tags", "gesis_tags");
		header = header.replace("textualReferences", "gesis_textual_references");
		header = header.replace("url", "url");/*in rg-schema*/
		header = header.replace("volume", "gesis_volume");
		if(entityType == EntityTypes.project)
			header = header.replace("year", "start_year");/*in rg-schema*/
		else
			header = header.replace("year", "publication_year");/*in rg-schema*/
		
		
		return header;
	}
	
	
	private void addResearchGraphValues(String[] orderedElements, HashMap<String, Integer> columnsWithIndexes, String local_id){
		Integer columnIndex = columnsWithIndexes.get("key");
		if(columnIndex != null && columnIndex >= 0)
			orderedElements[columnIndex] = "researchgraph.org/gesis/" + local_id;
		
		columnIndex = columnsWithIndexes.get("source");
		if(columnIndex != null && columnIndex >= 0)
			orderedElements[columnIndex] = "gesis.org";
		
		columnIndex = columnsWithIndexes.get("last_updated");
		if(columnIndex != null && columnIndex >= 0)
			orderedElements[columnIndex] = this.currentDateAndTime;
		
		columnIndex = columnsWithIndexes.get("scopus_eid");
		if(columnIndex != null && columnIndex >= 0)
			orderedElements[columnIndex] = "\"\"";
		
		columnIndex = columnsWithIndexes.get("scopus_eid");
		if(columnIndex != null && columnIndex >= 0)
			orderedElements[columnIndex] = getLabel(this.currentEntityType);
	}
	
	
//	### labels ###
	public String getLabel(String entityName){
		String label = "";
		EntityTypes entityType = EntityTypes.valueOf(entityName);
		
		switch(entityType){
		case citedData: label = "gesis|citedData"; break;
		case dataset: label = "dataset"; break;
		case institution: label = "institution"; break;
		case instrument: label = "gesis|instrument"; break;
		case project: label = "grant"; break;
		case publication: label = "publication";
		}		
		
		return label;
	}
	
//	### merge csv files #########################################
	public void mergeCsvFiles(String csvDirPath, String csvOutputPath){
		TreeSet<String> columns = getAllColumnsFromHeaders(csvDirPath);
		HashSet<String> entityTypes = new HashSet<>();
		
		String csvOutputPathPrefix = csvOutputPath.substring(0, csvOutputPath.lastIndexOf("."));
		String csvOutputPathSuffix = csvOutputPath.substring(csvOutputPath.lastIndexOf("."));
		String currentTypeOutputPath;
		
		File[] files = Tools.getFilesFromDir(csvDirPath);
		String content, header;
		String[] fileColumns;
		String[] rows;
		
		String orderedRowContent;
		
		File outFile;
		
		for(File inFile: files){
			try {
				content = FileUtils.readFileToString(inFile, "UTF-8");
				
				header = content.substring(0, content.indexOf("\n"));
				fileColumns = header.split(csvSeparator);
				
				content = content.substring(content.indexOf("\n") +1);
				rows = content.split("\n");
				
				try {
					for(String row: rows){
						orderedRowContent = getOrderedRow(row, fileColumns) + "," + getLabel(this.currentEntityType) + "\n";
						currentTypeOutputPath = csvOutputPathPrefix + "_" + currentEntityType + csvOutputPathSuffix;
						outFile = new File(currentTypeOutputPath);
						
//						add header if it's the first occurrence of the current entityType
						if(entityTypes.add(this.currentEntityType))
							FileUtils.writeStringToFile(outFile, this.headerMap.get(this.currentEntityType) + ",:LABEL" + "\n", "UTF-8");
						
						FileUtils.writeStringToFile(outFile, orderedRowContent, "UTF-8", true);
					}				
				} catch (IOException e1) {
					e1.printStackTrace();
				}				
				
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		
	}	
	
	private String currentEntityType = "";
	public String getOrderedRow(String unorderedRow, String[] fileHeader){
		CSVCleaner cleaner = new CSVCleaner();		
		ArrayList<String> rowElements = cleaner.getElements(unorderedRow);	
		
		this.currentEntityType = getCurrentEntityType(fileHeader, rowElements);
		HashMap<String, Integer> columnsWithIndexes = this.columnIndexMap.get(this.currentEntityType);
		
		String[] orderedElements = Tools.getInitializedStringArray(columnsWithIndexes.size());
		Integer columnIndex;
		String local_id = "";
		
		for(int i = 0; i < fileHeader.length; i++){
			columnIndex = columnsWithIndexes.get(fileHeader[i].trim());
			if(columnIndex != null && columnIndex >= 0){
				orderedElements[columnIndex] = rowElements.get(i).trim();
				
				if(fileHeader[i].contains("gwsId"))
					local_id = orderedElements[columnIndex];
			}
		}
		
		addResearchGraphValues(orderedElements, columnsWithIndexes, local_id);
		
		return Tools.getArrayAsString(orderedElements, csvSeparator);
	}
	
	private String getCurrentEntityType(String[] fileHeader, ArrayList<String> rowElements){
		for(int i = 0; i < fileHeader.length; i++){
			if(fileHeader[i].equals("_source.entityType"))
				return rowElements.get(i).trim();
		}
		
		return null;
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
