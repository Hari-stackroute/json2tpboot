package demo.json2tpboot;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.steelbridgelabs.oss.neo4j.structure.Neo4JElementIdProvider;
import com.steelbridgelabs.oss.neo4j.structure.Neo4JGraph;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.core.schema.VertexLabelMaker;
import org.neo4j.driver.v1.*;
import org.sunbird.cassandra.CassandraOperation;
import org.sunbird.common.Response;
import org.sunbird.helper.CassandraConnectionManager;
import org.sunbird.helper.CassandraConnectionMngrFactory;
import org.sunbird.helper.ServiceFactory;
import org.umlg.sqlg.structure.SqlgGraph;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class TPUtils {
    public static enum DBTYPE {NEO4J, POSTGRES, CASSANDRA};

    private static Driver driver;
    private DBTYPE target;

    private static void initDriver(boolean withAuth, String databaseHost, String databasePort) {
        Config.ConfigBuilder configBuilder = Config.build().withConnectionTimeout(1, TimeUnit.MINUTES)
                .withMaxIdleSessions(10)
                .withConnectionLivenessCheckTimeout(30, TimeUnit.SECONDS);
        Config config = configBuilder.toConfig();

        if (withAuth) {
            AuthToken authToken = AuthTokens.basic("neo4j", "stackroute1!");
            driver = GraphDatabase.driver(String.format("bolt://%s:%s", databaseHost, databasePort),
                    authToken, config);
        } else {
            driver = GraphDatabase.driver(String.format("bolt://%s:%s", databaseHost, databasePort),
                    AuthTokens.none(), config);
        }
    }

    public static Graph createNeo4jGraph() {
        boolean withAuth = true;
        String databaseHost = "localhost";
        String databasePort = "11010";
        Neo4JGraph neo4JGraph;

        if (driver == null) {
            initDriver(withAuth, databaseHost, databasePort);
        }
        Neo4JElementIdProvider<?> idProvider = new RecordIdProvider();
        neo4JGraph = new Neo4JGraph(driver, idProvider, idProvider);

        return neo4JGraph;
    }

    public static Graph createPostgresGraph() {
        String jdbcUrl = "jdbc:postgresql://localhost:5432/json2tp";
        String jdbcUsername = "postgres";
        String jdbcPassword = "postgres";
        Configuration config = new BaseConfiguration();
        config.setProperty("jdbc.url", jdbcUrl);
        config.setProperty("jdbc.username", jdbcUsername);
        config.setProperty("jdbc.password", jdbcPassword);
        SqlgGraph graph = SqlgGraph.open(config);
        return graph;
    }

    public static Graph createJanusCassandraGraph() {
        Configuration config = new BaseConfiguration();
		/*config.setProperty("jdbc.url", connectionInfo.getUri());
		config.setProperty("jdbc.username", connectionInfo.getUsername());
		config.setProperty("jdbc.password", connectionInfo.getPassword());*/
        config.setProperty("storage.backend", "cql");
        //config.setProperty("query.batch", true);

        //String host = environment.getProperty("cassandra.hostname");
        config.setProperty("storage.hostname", "127.0.0.1");
        config.setProperty("storage.cql.keyspace", "tp_janus1");
        config.setProperty("storage.cql.compact-storage",false);
        config.setProperty("storage.cql.compression",false);

        JanusGraph graph = JanusGraphFactory.open(config);
        return graph;
    }

    public static Graph getGraph(DBTYPE target) {
        switch (target) {
            case NEO4J:
                return createNeo4jGraph();
            case POSTGRES:
                return createPostgresGraph();
            case CASSANDRA:
                return createJanusCassandraGraph();
            default:
                return null;
        }
    }

    public static String createLabel() {
        return UUID.randomUUID().toString();
    }

    public static Vertex createVertex(Graph graph, String label, Vertex parentVertex, JsonNode jsonObject) {
        Vertex vertex = graph.addVertex(label);
        vertex.property("osid", vertex.id());
        if (jsonObject != null) {
            jsonObject.fields().forEachRemaining(entry -> {
                JsonNode entryValue = entry.getValue();
                if (entryValue.isValueNode()) {
                    vertex.property(entry.getKey(), entryValue.asText());
                } else if (entryValue.isObject()) {
                    createVertex(graph, entry.getKey(), vertex, entryValue);
                }
            });
        }
        if (parentVertex != null) {
            addEdge(graph, label, parentVertex, vertex);
        }
        return vertex;
    }

    public static Edge addEdge(Graph graph, String label, Vertex v1, Vertex v2) {
        return v1.addEdge(label, v2);
    }

    public static Vertex createParentVertex(Graph graph, String parentGroupName) {
        GraphTraversalSource gtRootTraversal = graph.traversal();
        GraphTraversal<Vertex, Vertex> rootVertex = gtRootTraversal.V().hasLabel(parentGroupName);
        Vertex parentVertex = null;
        if (!rootVertex.hasNext()) {
            parentVertex = createVertex(graph, parentGroupName, null, null);
        } else {
            parentVertex = rootVertex.next();
        }

        return parentVertex;
    }

    public static List<String> verticesCreated = new ArrayList<String>();

    public static Vertex processNode(Graph graph, String entityType, Vertex parentVertex, JsonNode node) {
        Vertex v = null;
        Iterator<Map.Entry<String, JsonNode>> entryIterator = node.fields();
        while (entryIterator.hasNext()) {
            Map.Entry<String, JsonNode> entry = entryIterator.next();
            if (entry.getValue().isValueNode()) {
                // Create properties
                System.out.println("Create properties within vertex " + entityType);
                System.out.println(entityType + ":" + entry.getKey() + " --> " + entry.getValue());
                parentVertex.property(entry.getKey(), entry.getValue());
            } else if (entry.getValue().isObject()) {
                v = createVertex(graph, entry.getKey(), parentVertex, entry.getValue());
                addEdge(graph, entityType, v, parentVertex);
            } else if (entry.getValue().isArray()) {
                // TODO
            }
        }
        return v;
    }

    void setTarget(DBTYPE dbtype) {
        target = dbtype;
    }


    public static JsonNode readGraph2Json(Graph graph, String osid) throws IOException, Exception {
        ObjectNode objectNode = JsonNodeFactory.instance.objectNode();

        Transaction tx = graph.tx();
        System.out.println(graph.vertices(osid).hasNext());

        Iterator<Vertex> itrV = graph.vertices(osid);
        if (itrV.hasNext()) {
            Vertex v = itrV.next();
            v.properties().forEachRemaining(prop -> {
                objectNode.put(prop.label().toString(), prop.value().toString());
            });
        }

//        StatementResult sr = neo4JGraph.execute("match (n) where n.osid='" + osid + "' return n");
//        while(sr.hasNext()) {
//            Record record = sr.single();
//            InternalNode internalNode = (InternalNode) record.get("n").asNode();
//            ObjectMapper mapper = new ObjectMapper();
//            JsonNode node = mapper.createObjectNode();
//            String label = internalNode.labels().iterator().next();
//            map.put(label, internalNode.asValue().asMap());
//        }
        tx.commit();
        return objectNode;
    }

    public static void main(String args[]) throws Exception {
        /*JanusGraph g = (JanusGraph) createJanusCassandraGraph();
        Transaction tx = g.tx();
        Vertex teacherV = g.addVertex("Teacher");
        teacherV.property("serialNum", 7);
        Vertex teacherRoleV = g.addVertex("TeacherRole");
        teacherRoleV.property("roleId",2);
        teacherV.addEdge("role",teacherRoleV);
        tx.commit();
        g.close();*/
        try{
            Map map = new HashMap();
            map.put("id", "4");
            map.put("serialNum", 1);
            map.put("teacherName","Marvin Pande");
            map.put("gender", "GenderTypeCode-MALE");
            map.put("socialCategory","SocialCategoryTypeCode-GENERAL");
            map.put("highestAcademicQualification","AcademicQualificationTypeCode-PHD");
            map.put("highestTeacherQualification","TeacherQualificationTypeCode-MED");
            map.put("yearOfJoiningService","2014");
            CassandraOperation cassandraOperation = ServiceFactory.getInstance();
            CassandraConnectionManager cassandraConnectionManager =
                    CassandraConnectionMngrFactory.getObject("standalone");
            boolean result =
                    cassandraConnectionManager.createConnection("127.0.0.1", "9042", null, null, "tpcass");
            Response response = cassandraOperation.insertRecord("tpcass","Teacher",map);
            System.out.println("Id:"+response.getId()+" result:"+response.getResult());
            System.out.println("params:"+response.getParams());
            Response res = cassandraOperation.getRecordById("tpcass","Teacher","1");
            System.out.println("response:"+res.getResult().toString());
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

}
