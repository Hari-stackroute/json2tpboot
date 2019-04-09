package demo.json2tpboot;

import org.sunbird.cassandra.CassandraOperation;
import org.sunbird.common.Response;
import org.sunbird.helper.CassandraConnectionManager;
import org.sunbird.helper.CassandraConnectionMngrFactory;
import org.sunbird.helper.ServiceFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class CassandraImpl {
    CassandraOperation cassandraOperation = null;
    CassandraConnectionManager cassandraConnectionManager = null;
    private static List<String> keyList = Arrays.asList("teachingRole");

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
            if(mapElements.containsKey(key)) {
                Map<String,Object> subMap = (Map<String, Object>) mapElements.get(key);
                Response response = cassandraOperation.insertRecord("tpcass",key,subMap);
            }
            mapElements.remove(key);
        });
        Response response = cassandraOperation.insertRecord("tpcass","Teacher",mapElements);
    }

    public Map<String, Object> readFromCassandra(Map<String, Object> mapObject) {
        String elementType = (String) mapObject.keySet().iterator().next();
        Map<String, Object> mapElement = (Map<String, Object>) mapObject.get(elementType);
        Response response = cassandraOperation.getRecordById("tpcass",elementType, (String) mapElement.get("teacherroleid"));
        return response.getResult();
    }
}
