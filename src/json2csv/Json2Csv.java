package json2csv;
import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.json.CDL;
import org.json.JSONArray;
import org.json.JSONObject;

public class Json2Csv {
	private HashSet<String> csvColumnNames;
	private HashSet<String> csvColumnNamesFirstLine;
	
	public Json2Csv(){
		csvColumnNames = new HashSet<>();
		csvColumnNamesFirstLine = new HashSet<>();
	}
	
	public String convertJson2Csv(String jsonString){
		String csv = "";

		JSONObject jsonObject = new JSONObject(jsonString).getJSONObject("hits");
		JSONArray jsonArray = jsonObject.getJSONArray("hits");
		String unnestedJsonString = getUnnestedJSONStructure(jsonArray, "hits");
		unnestedJsonString = addMissingColumnNames(unnestedJsonString);
		jsonArray = new JSONArray(unnestedJsonString);
		csv = CDL.toString(jsonArray);
		
		return csv;
	}
	
		
	private String getUnnestedJSONStructure(JSONArray jsonArray, String arrayName){
		csvColumnNames = new HashSet<>();
		csvColumnNamesFirstLine = new HashSet<>();
				
		arrayName = arrayName.isEmpty() ? "default" : arrayName;
		String unnestedJSONStructure = unnestJSONStructure(jsonArray, "");		
		
		return "[" + unnestedJSONStructure + "]";
	}
	
	private String addMissingColumnNames(String jsonString){
		String columnNameJsonString = getMissingColumnNamesJSONString();
		
		int index = jsonString.indexOf("}");
		jsonString = jsonString.substring(0, index) + columnNameJsonString + jsonString.substring(index);
				
		return jsonString;
	}
	
	private String getMissingColumnNamesJSONString(){
		if(this.csvColumnNames.size() == this.csvColumnNamesFirstLine.size())
			return "";
		
		String columnNameJsonString = "";		
		
		for(String columnName: this.csvColumnNames){
			if(!this.csvColumnNamesFirstLine.contains(columnName)){
				columnNameJsonString += ",\"" + columnName + "\":\"\"";
			}
		}
		
		return columnNameJsonString;
	}
	
	private String unnestJSONStructure(JSONArray jsonArray, String arrayName){
		String arrayJsonString = "";
					
		for(int i = 0; i < jsonArray.length(); i++){		
			if(arrayName.isEmpty()){
				arrayJsonString += "{";						
			}
			
			JSONObject subObject = jsonArray.optJSONObject(i);
			
			Set<String> keys = subObject.keySet();
			String valueString;
			
			String currentElementName;
			for(String key: keys){				
				currentElementName = (arrayName.isEmpty() ? "" : (arrayName + ".")) + key;
				this.csvColumnNames.add(currentElementName);
				
				valueString = subObject.get(key).toString();
				valueString = escapeSpecialCharacters(valueString);

				if(valueString.startsWith("{") && valueString.endsWith("}")){

//					System.out.println(key + ": " + valueString);
					valueString = "[" + valueString + "]";
					
					JSONArray subArray = new JSONArray(valueString);
					arrayJsonString += unnestJSONStructure(subArray, currentElementName);
				}
				else{
					arrayJsonString += "\"" + currentElementName + "\": \"" + valueString + "\",";
				}					
			}
			
			if(arrayName.isEmpty()){
				arrayJsonString = arrayJsonString.substring(0, arrayJsonString.trim().length() -1) + "}";
				
				if(i != (jsonArray.length() -1))
					arrayJsonString += ",";				
			}
			
//			remember the key values, that the first hit contained
			if(i == 0 && arrayName.isEmpty()){
				this.csvColumnNamesFirstLine.addAll(this.csvColumnNames);
			}
		}
		
		
		
		return arrayJsonString;
	}
	
	private String escapeSpecialCharacters(String text){	
		text = text.replace("\n", " ");
		
		text = text.replace("\\\\hbox", "");
		text = text.replaceAll("\\\\.([A-Za-z &ÄäÖöÜüß_%])", "$1");
		text = text.replaceAll("\\\\.([A-Za-z &ÄäÖöÜüß_%])", "$1");
		
		
		if(text.startsWith("[") && text.endsWith("]") && !text.contains(":")){		
			text = text.substring(1, text.length()-1);
			text = text.replace("\",\"", "\"; \"");
		}
		
		if(!text.startsWith("{")){
			//remove already contained escapes
			text = text.replace("\\\"", "\"");
			//add new escapes
			text = text.replace("\"", "\\\"");
		}
		
		return text;
	}
	
	
	public void transformAndSave(String jsonString, String csvOutputPath) throws IOException{
		File csvFile = new File(csvOutputPath);
		Json2Csv j2c = new Json2Csv();
		String csv;			
		
		csv = j2c.convertJson2Csv(jsonString);
		FileUtils.writeStringToFile(csvFile, csv, "UTF-8");
	}
	
	public void doTransformation(String url, String csvOutputPath){
		
		String jsonString = "";
		try {
			ElasticSearchRequest esRequest = new ElasticSearchRequest();
			int numHits = esRequest.getTotalHitCount(url);
			if(numHits < 0){
				numHits = esRequest.getTotalHitCountFromJson(url);
			}
			
			int startValue = esRequest.getStartValue(url);
			if(startValue < 0)
				startValue = 0;
			int currentHits = 0;				
			
			int size = 500;				
			if(numHits < size)
				size = numHits;
			
			String outputPrefix = csvOutputPath.substring(0, csvOutputPath.lastIndexOf("."));
			String outputSuffix = csvOutputPath.substring(csvOutputPath.lastIndexOf("."));
			while(currentHits < numHits){
				csvOutputPath = outputPrefix + "_" + (startValue + currentHits) + outputSuffix;
				jsonString = (new ElasticSearchRequest()).getJSONString(url, (startValue + currentHits), size);
				transformAndSave(jsonString, csvOutputPath);
				currentHits += size;
			}						
			
		} catch (IOException e1) {
			e1.printStackTrace();
		} catch(URISyntaxException e2){
			e2.printStackTrace();
		}
	}    
}

