package demo.json2tpboot;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.time.Duration;
import java.time.Instant;

public class CreateRecord extends Thread {
   // int id;
    Vertex groupV;
    String entityType;
    TPUtils.DBTYPE target;
    JsonNode rootNode;

    public CreateRecord(JsonNode rootNode, String entityType, TPUtils.DBTYPE target, Vertex parentVertex) {
        this.rootNode = rootNode;
        this.entityType = entityType;
        this.target = target;
       // this.id = i;
        this.groupV = parentVertex;

    }

    public void insert(){
        //System.out.println(id + " Create record");
        Instant startTime = Instant.now();
        Vertex v = null;
        try {
            try (Graph graph = TPUtils.getGraph(target)) {
                try {
                    try (Transaction tx = graph.tx()) {
                        v = TPUtils.processNode(graph, entityType, groupV, rootNode);
                        tx.commit();
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }

                System.out.println(TPUtils.readGraph2Json(graph, v.id().toString()));
            }
        } catch (Exception e) {
            System.out.println("Can't run this instance of the thread");
        }
        Instant endTime = Instant.now();
       /* System.out.println(id + "," +
                Duration.between(startTime, endTime).toNanos() + "," +
                Duration.between(startTime, endTime).toMillis());*/
    }

    /*@Override
    public void run() {
        System.out.println(id + " Create record");
        Instant startTime = Instant.now();
        Vertex v = null;
        try {
            try (Graph graph = TPUtils.getGraph(target)) {
                try {
                    try (Transaction tx = graph.tx()) {
                        v = TPUtils.processNode(graph, entityType, groupV, rootNode);
                        tx.commit();
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }

                System.out.println(TPUtils.readGraph2Json(graph, v.id().toString()));
            }
        } catch (Exception e) {
            System.out.println("Can't run this instance of the thread");
        }
        Instant endTime = Instant.now();
        System.out.println(id + "," +
                Duration.between(startTime, endTime).toNanos() + "," +
                Duration.between(startTime, endTime).toMillis());
    }*/
}
