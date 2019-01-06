package demo.json2tpboot;

import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonSetter;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.Map;

public class Request {

	@JsonSetter("request")
	private Map<String, Object> requestMap;
	@JsonIgnore
	private String requestMapString;
	@JsonIgnore
	private JsonNode requestMapNode;

	public Request(){

	}


	public Request( Map<String, Object> requestMap) {
		this.requestMap = requestMap;
	}

	@JsonGetter("request")
	public Map<String, Object> getRequestMap() {
		return requestMap;
	}

	/**
	 * Gets the root entity type name for this payload.
	 * @return
	 */
	public String getEntityType() {
		return requestMap.keySet().iterator().next().toString();
	}

	public String getRequestMapAsString() {
		if (requestMapString == null) {
			try {
				requestMapString = new ObjectMapper().writeValueAsString(getRequestMap());
			} catch (JsonProcessingException jpe) {
				requestMapString = "";
			}
		}
		return requestMapString;
	}

	public void setRequestMap(Map<String, Object> requestMap) {
		this.requestMap = requestMap;
	}

	public JsonNode getRequestMapNode() {
		if (requestMapNode == null || requestMapNode.isNull()) {
			requestMapNode = new ObjectMapper().valueToTree(requestMap);
		}
		return requestMapNode;
	}

}
