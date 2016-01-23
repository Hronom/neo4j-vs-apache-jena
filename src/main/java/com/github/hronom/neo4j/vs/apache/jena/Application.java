package com.github.hronom.neo4j.vs.apache.jena;

import com.github.hronom.neo4j.vs.apache.jena.fillers.ApacheJenaFiller;
import com.github.hronom.neo4j.vs.apache.jena.fillers.Filler;
import com.github.hronom.neo4j.vs.apache.jena.fillers.Neo4jFiller;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class Application {
    private static final Logger logger = LogManager.getLogger();

    private static final long totalCount = 1000;

    public static void main(String args[]) {
        // Test Neo4j.
        {
            logger.info("Filling Neo4j...");
            Neo4jFiller neo4jFiller = new Neo4jFiller();
            neo4jFiller.initialize();

            long begin = System.currentTimeMillis();
            fill(neo4jFiller);
            long end = System.currentTimeMillis();
            logger.info("Neo4j time: " + (end - begin) + " ms.");
        }

        // Test Apache Jena.
        {
            logger.info("Filling Apache Jena...");
            ApacheJenaFiller apacheJenaFiller = new ApacheJenaFiller();
            apacheJenaFiller.initialize();

            long begin = System.currentTimeMillis();
            fill(apacheJenaFiller);
            long end = System.currentTimeMillis();
            logger.info("Apache Jena time: " + (end - begin) + " ms.");
        }
    }

    private static boolean fill(Filler filler) {
        long beginTime = System.currentTimeMillis();

        for (long i = 2; i < totalCount; i++) {
            if (!filler.insert("Tag" + i, "Tag" + (i + 1))) {
                return false;
            }

            if (!filler.insert("Tag" + (i - 1), "Tag" + (i - 2))) {
                return false;
            }

            long currentTime = System.currentTimeMillis();
            if ((currentTime - beginTime) > 3000) {
                beginTime = System.currentTimeMillis();
                logger.info("Count of inserted tags: " + i);
            }
        }

        logger.info("Count of inserted tags: " + totalCount);

        return true;
    }
}
