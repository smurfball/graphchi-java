package edu.cmu.graphchi.preprocessing;

import java.io.*;
import java.nio.*;
import java.util.concurrent.*;

public class MMapReader {
    static private final int EDGES_TO_BUFFER = 100 * 1024;

    static private final byte LF = 10;
    static private final byte CR = 13;
    static private final byte HT = 9;
    static private final byte SPACE = 32;
    static private final byte ZERO = 48;
    static private final byte HASH = 35;

    static private final ThreadFactory threadFactory = new ThreadFactory() {
            public Thread newThread(Runnable r) {
                Thread t = new Thread(r);
                t.setDaemon(true);
                return t;
            }
        };

    static private final int NUM_READERS = 2 * Runtime.getRuntime().availableProcessors();

    static private final ExecutorService executor = Executors.newFixedThreadPool(NUM_READERS,
										threadFactory);
    //static private final ExecutorService executor2 = Executors.newCachedThreadPool(threadFactory);

    public static void unmap(MappedByteBuffer buffer) {
	sun.misc.Cleaner cleaner = ((sun.nio.ch.DirectBuffer) buffer).cleaner();
	cleaner.clean();
    }

    public void readWithMmap(RandomAccessFile file, int numShards, EdgeHandler handler) throws Exception {
	readWithMmap(file, 1024 * 1024 * 1000l, numShards, handler);
    }

    public void readWithMmap(final RandomAccessFile file, long chunkSize, final int numShards, final EdgeHandler handler) throws Exception {
	long start = System.currentTimeMillis();

	final long fileSize = file.length();
	final int nChunks = (int) Math.ceil( ((double)fileSize) / chunkSize);

	final CountDownLatch latch = new CountDownLatch(nChunks);

	for (int i = 0; i < nChunks; i++) {
	    final int chunk = i;
	    final long byteStart = chunk * chunkSize;
	    final long nBytes = Math.min(fileSize, byteStart + chunkSize) - byteStart;

	    executor.submit(new Runnable() {
		    public void run() {
			int numEdgesProcessed = 0;
			
			//System.err.println("CHUNK " + chunk + " " + byteStart + " " + nBytes + " " + fileSize + " " + nChunks);

			try {
			    int[] currentBufIndex = new int[numShards];
			    final int[][] buf = new int[numShards][];
			    final int[] n = new int[2];
			    
			    for (int i = 0; i < numShards; i++) {
				buf[i] = new int[EDGES_TO_BUFFER * 2];
			    }

			    final MappedByteBuffer buffer = file.getChannel().map(java.nio.channels.FileChannel.MapMode.READ_ONLY, byteStart, nBytes);
			    
			    try {
				//executor2.submit(new Runnable() { public void run() { buffer.load(); } });
				
				boolean ignoreLine = false;
				boolean eatingWhitespace = false;
				int x = 0;
				for (int j = 0; j < nBytes; j++) {
				    byte c = buffer.get();

				    /*if (chunk == 0) System.err.println("ZZZ " + c + " " + (c==LF) + " " + (c==CR)
								       + " " + (c==SPACE) + " " + (c==HT)
								       + " " + (c==HASH) + " " + (c-ZERO));*/
				    
				    switch (c) {
				    case LF: case CR:
					if (!ignoreLine && x == 1) {
					    // System.out.println(n[0] + " " + n[1]);
					    int from = n[0];
					    int to = n[1];

					    int shard = to % numShards;
					    buf[shard][currentBufIndex[shard]] = from;
					    buf[shard][currentBufIndex[shard] + 1] = to;
					    currentBufIndex[shard] += 2;
					    numEdgesProcessed++;

					    if (currentBufIndex[shard] >= buf[shard].length) {
						handler.addEdges(buf[shard], currentBufIndex[shard], shard);
						currentBufIndex[shard] = 0;
					    }
					}
					
					ignoreLine = false;
					eatingWhitespace = false;
					x = 0;
					n[0] = n[1] = 0;
					break;
					
				    case SPACE: case HT:
					if (!eatingWhitespace) {
					    x++;
					    eatingWhitespace = true;
					}
					break;
					
				    case HASH:
					ignoreLine = true;
					break;
					
				    default:
					eatingWhitespace = false;
					n[x] = n[x] * 10 + (c - ZERO);
				    }
				}

				for (int shard = 0; shard < numShards; shard++) {
				    if (currentBufIndex[shard] > 0) {
					handler.addEdges(buf[shard], currentBufIndex[shard], shard);
				    }
				}

			    } catch (Exception e) {
				System.err.println("OOPS " + e.getLocalizedMessage());
				e.printStackTrace();
			    } finally {
				try {
				    unmap(buffer);
				} catch (Exception e) {
				    System.err.println("OOPS " + e.getLocalizedMessage());
				    e.printStackTrace();
				}
			    }
			} catch (Exception e) {
			    System.err.println("OOPS " + e.getLocalizedMessage());
			    e.printStackTrace();

			} finally {
			    System.err.println("DONE " + chunk + " " + numEdgesProcessed + " " + byteStart + " " + nBytes + " " + fileSize + " " + nChunks);
			    latch.countDown();
			}
		    }
		});
	}

	latch.await();
	long end = System.currentTimeMillis();
	System.err.println("ALL_DONE in " + (((double) (end-start)) / 1000d / 60d) + " minutes");
    }

    static public final class NullEdgeHandler implements EdgeHandler {
	public void addEdges(int[] edges, int nEdges, int shard) {
	}
    }

    static public void main(String[] args) throws Exception {
	try {
	    new MMapReader().readWithMmap(new RandomAccessFile(args[0], "r"), 1024 * 1024 * 1000l, 1, new NullEdgeHandler());

	} catch (Exception e) {
	    e.printStackTrace();
	} finally {
	    //executor.shutdownNow();
	}
    }
}
