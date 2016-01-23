package com.github.hronom.neo4j.vs.apache.jena.fillers;

public interface Filler {
    boolean initialize();

    boolean insert(String tagNameA, String tagNameB);
}
