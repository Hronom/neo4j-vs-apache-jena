package com.github.hronom.neo4j.vs.apache.jena.fillers;

import org.apache.jena.graph.Graph;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.tdb.TDBFactory;

public class ApacheJenaFiller implements Filler {
    private Graph graph;

    @Override
    public boolean initialize() {
        DatasetGraph datasetGraph = TDBFactory.createDatasetGraph("test_apache_jena_tdb");
        graph = datasetGraph.getDefaultGraph();
        return true;
    }

    @Override
    public boolean insert(String tagNameA, String tagNameB) {
        Triple triple = new Triple(
            NodeFactory.createURI("http://www.test.org/tag/" + tagNameA),
            NodeFactory.createURI("http://www.test.org/relatedTo"),
            NodeFactory.createURI("http://www.test.org/tag/" + tagNameB)
        );
        graph.add(triple);
        return true;
    }
}
