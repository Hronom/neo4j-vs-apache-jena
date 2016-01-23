package com.github.hronom.neo4j.vs.apache.jena.fillers;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.graphdb.schema.Schema;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

public class Neo4jFiller implements Filler {
    private static final Logger logger = LogManager.getLogger();

    private Path path = Paths.get("test_neo4j");

    private final Label tagLabel = Label.label("Tag");

    private final String tagNameProperty = "tagName";

    private GraphDatabaseService neo4jDatabase;

    private enum RelationshipTypes implements RelationshipType {
        relation
    }

    @Override
    public boolean initialize() {
        // Open database.
        neo4jDatabase = new GraphDatabaseFactory().newEmbeddedDatabase(path.toFile());

        // Registers a shutdown hook for the Neo4j instance so that it shuts down nicely when the VM
        // exits (even if you "Ctrl-C" the running application).
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                neo4jDatabase.shutdown();
            }
        });

        IndexDefinition indexDefinition;
        try (Transaction tx = neo4jDatabase.beginTx()) {
            Schema schema = neo4jDatabase.schema();
            Iterator<IndexDefinition> iter = schema.getIndexes(tagLabel).iterator();
            if (!iter.hasNext()) {
                indexDefinition = schema.indexFor(tagLabel).on(tagNameProperty).create();
            } else {
                indexDefinition = iter.next();
            }
            tx.success();
        }

        try (Transaction tx = neo4jDatabase.beginTx()) {
            Schema schema = neo4jDatabase.schema();
            schema.awaitIndexOnline(indexDefinition, 30, TimeUnit.SECONDS);
            tx.success();
        }

        return true;
    }

    /**
     * About indexes http://neo4j.com/docs/3.0.0-M02/tutorials-java-embedded-new-index.html
     */
    @Override
    public boolean insert(String tagNameA, String tagNameB) {
        try (Transaction tx = neo4jDatabase.beginTx()) {
            Node tagNameANode = neo4jDatabase.findNode(tagLabel, tagNameProperty, tagNameA);
            if (tagNameANode == null) {
                tagNameANode = neo4jDatabase.createNode(tagLabel);
                tagNameANode.setProperty(tagNameProperty, tagNameA);
            }

            Node tagNameBNode = neo4jDatabase.findNode(tagLabel, tagNameProperty, tagNameB);
            if (tagNameBNode == null) {
                tagNameBNode = neo4jDatabase.createNode(tagLabel);
                tagNameBNode.setProperty(tagNameProperty, tagNameB);
            }

            boolean created = false;
            for (Relationship r : tagNameANode.getRelationships(RelationshipTypes.relation)) {
                if (r.getOtherNode(tagNameANode).equals(tagNameBNode)) {
                    created = true;
                    break;
                }
            }
            if (!created) {
                tagNameANode.createRelationshipTo(tagNameBNode, RelationshipTypes.relation);
            }
            tx.success();
        }

        return true;
    }
}
