package demo.json2tpboot;

import org.sunbird.cassandra.CassandraOperation;
import org.sunbird.common.Response;
import org.sunbird.helper.CassandraConnectionManager;
import org.sunbird.helper.CassandraConnectionMngrFactory;
import org.sunbird.helper.ServiceFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CassandraImpl {
    CassandraOperation cassandraOperation = null;
    CassandraConnectionManager cassandraConnectionManager = null;
    private static List<String> keyList = Arrays.asList("teachingRole","basicProficiencyLevel");

    CassandraImpl() {
        cassandraOperation = ServiceFactory.getInstance();
        cassandraConnectionManager =
                CassandraConnectionMngrFactory.getObject("standalone");
        boolean result =
                cassandraConnectionManager.createConnection("127.0.0.1", "9042", null, null, "tpcass");
    }

    public void addToCassandra(Map<String, Object> mapObject) {
        Map<String, Object> mapElements = (Map<String, Object>) mapObject.get("Teacher");
        keyList.forEach(key -> {
            String id = null;
            Response response = null;
            if(mapElements.containsKey(key)) {
                Map<String,Object> subMap = (Map<String, Object>) mapElements.get(key);
                response = cassandraOperation.insertRecord("tpcass",key,subMap);
                id = (String) subMap.get("id");
            }
            mapElements.remove(key);
            mapElements.put(key+"Id",id);
        });
        Response response = cassandraOperation.insertRecord("tpcass","Teacher",mapElements);
    }

    public Map<String, Object> readFromCassandra(Map<String, Object> mapObject) {
        Map<String,Object> responseMap = new HashMap<>();
        String elementType = (String) mapObject.keySet().iterator().next();
        Map<String, Object> mapElement = (Map<String, Object>) mapObject.get(elementType);
        Response response = cassandraOperation.getRecordById("tpcass",elementType, (String) mapElement.get("id"));
        Map<String, Object> result  = response.getResult();
        List<HashMap<String,Object>> lst = (List<HashMap<String, Object>>) response.getResult().get("response");
        Map<String,Object> entityMap = lst.get(0);
        responseMap.put(elementType,entityMap);
        keyList.forEach(key -> {
            if(entityMap.containsKey((key+"Id").toLowerCase())) {
                Response subResponse = cassandraOperation.getRecordById("tpcass",key, (String) entityMap.get((key+"id").toLowerCase()));
                List<HashMap<String,Object>> sublst = (List<HashMap<String, Object>>) subResponse.getResult().get("response");
                Map<String,Object> subentityMap = sublst.get(0);
                entityMap.remove((key+"id").toLowerCase());
                entityMap.put(key,subentityMap);
            }
        });
        return responseMap;
    }
}
