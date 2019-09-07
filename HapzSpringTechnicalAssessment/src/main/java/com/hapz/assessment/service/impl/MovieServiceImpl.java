package com.hapz.assessment.service.impl;

/* DO NOT CHANGE */
 /* Please do not change the imported functions as you will be assessed based on your usage of the selected libraries, but you can import your own class */
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

import org.apache.commons.lang3.ArrayUtils;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.hapz.assessment.model.SearchCache;
import com.hapz.assessment.model.SearchCacheMovie;
import com.hapz.assessment.repository.SearchCacheRepository;
import com.hapz.assessment.service.MovieService;
import com.hapz.assessment.service.constants.MovieConstants;

/* Please do not change the imported functions as you will be assessed based on your usage of the selected libraries, but you can import your own class */
 /* DO NOT CHANGE */
@Service
public class MovieServiceImpl implements MovieService {

    private SearchCacheRepository searchCacheRepository;
    
    private static final Logger LOGGER = LoggerFactory.getLogger(MovieServiceImpl.class);

    public MovieServiceImpl(SearchCacheRepository searchCacheRepository) {
        this.searchCacheRepository = searchCacheRepository;
    }

    @Override
    public String[] getMovieTitles(String query){
        String[] movieTitles = this.getMovieTitlesFromCache(query);
        if (movieTitles == null) {
            movieTitles = this.getMovieTitlesFromApi(query);

            this.storeMovieTitlesInCache(query, movieTitles);
        }

        return movieTitles;
    }

    /**
     * Retrieves a unique, sorted list of movie titles from the API using the
     * provided query string
     *
     * @return unique, sorted list of movie titles
     */
    private String[] getMovieTitlesFromApi(String query) {
        // TODO 1: Implement function to retrieve a unique, sorted list of movie titles from the API using the provided query string
    
    	int i = 1;
    	StringBuilder requestURL;
    	StringBuilder response = new StringBuilder();
    	String[] titles = {};
    	JSONArray value = new JSONArray();
    	
    	try{
    	String baseUrl = MovieConstants.BASE_URL.concat(URLEncoder.encode(String.valueOf(query), "UTF-8"));
    	do{
    		String[] titleList = new String[250];
    		requestURL = new StringBuilder(baseUrl.replace(MovieConstants.PAGE_PARAM.concat(String.valueOf(i-1)), ""));
    		requestURL.append(MovieConstants.PAGE_PARAM).append(URLEncoder.encode(String.valueOf(i), "UTF-8"));
    	    URL urlForGetRequest = new URL(String.valueOf(requestURL));
    	    String readLine;
    	    HttpURLConnection conection = (HttpURLConnection) urlForGetRequest.openConnection();
    	    conection.setRequestMethod("GET");
    	    int responseCode = conection.getResponseCode();
    	    if (responseCode == HttpURLConnection.HTTP_OK && conection.getInputStream() != null) {
    	        BufferedReader in = new BufferedReader(new InputStreamReader(conection.getInputStream()));
    	        while ((readLine = in .readLine()) != null) {
    	            response.append(readLine);
    	        } 
    	        in .close();
    	        value = getTitlesFromData(response, titleList);
    	        response.setLength(0);
    	        titles = ArrayUtils.addAll(titles, titleList);
    	        
        } else {
            LOGGER.error("No Movie Found" + responseCode);
        }
        i++;
    	}while(value.length()>1);
    	
        titles = new HashSet<String>(Arrays.asList(titles)).toArray(new String[0]);
        titles = Arrays.stream(titles)
                .filter(s -> (s != null && s.length() > 0))
                .toArray(String[]::new); 
        
        Arrays.sort(titles);
    	}catch(Exception e){
    		LOGGER.error(e.getMessage());
    	}
		return titles;
    }

	private JSONArray getTitlesFromData(StringBuilder response,
			String[] titleList) throws JSONException {
		JSONArray value;
		JSONObject jObject = new JSONObject(String.valueOf(response));
		value = (JSONArray) jObject.get("data");
		for(int x = 0; x < value.length(); x++){
			titleList[x] = value.getJSONObject(x).getString("Title");
		}
		return value;
	}

    /**
     * Stores the provided list of movie titles a unique, sorted list of movie titles from the database using
     * the provided query string
     *
     * @
     * @return unique, sorted list of movie titles
     */
    private void storeMovieTitlesInCache(String query, String[] movieTitles) {
        // TODO 2: Implement function to store the list of movie titles in the SearchCache repository
    	   SearchCacheMovie searchResult;
    	   List<SearchCacheMovie> resultList = new ArrayList<>();
    	   for(int i=0; i < movieTitles.length; i++){
    		   searchResult = new SearchCacheMovie();
    		   searchResult.setMovieTitle(movieTitles[i]);
    		   resultList.add(searchResult);
    	   }
    	   
    	   searchCacheRepository.save(new SearchCache(query, resultList));
    	   LOGGER.info("resultList Pushed");
    }

    /**
     * Retrieves a unique, sorted list of movie titles from the database using
     * the provided query string
     *
     * @return unique, sorted list of movie titles
     */
    private String[] getMovieTitlesFromCache(String query) {
        // TODO 3: Implement function to retrieve a unique, sorted list of movie titles from the cache using the provided query string
	       return searchCacheRepository.findAll()
	                .stream()
	                .filter(searchCache -> query.equals(searchCache.getQuery()))
	                .findFirst()
	                .map(SearchCache::getSearchResults)
	                .map(searchCacheMovies -> searchCacheMovies.stream().map(SearchCacheMovie::getMovieTitle).toArray(String[]::new))
	                .orElse(null);
    }

}
