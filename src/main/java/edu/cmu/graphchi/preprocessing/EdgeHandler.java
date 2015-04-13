package edu.cmu.graphchi.preprocessing;

public interface EdgeHandler {
    /**
     * @param edges
     * @param nEdges
     * @param shard
     */
    void addEdges(int[] edges, int nEdges, int shard) throws java.io.IOException;
}
