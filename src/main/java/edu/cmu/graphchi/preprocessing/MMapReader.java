package edu.cmu.graphchi.preprocessing;

import java.io.*;
import java.nio.*;
import java.util.concurrent.*;

public class MMapReader {
    static private final int EDGES_TO_BUFFER = 100000;

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

    static private final ExecutorService executor1 = Executors.newFixedThreadPool(2 * Runtime.getRuntime().availableProcessors(),
										  threadFactory);
    //static private final ExecutorService executor2 = Executors.newCachedThreadPool(threadFactory);

    public static void unmap(MappedByteBuffer buffer) {
	sun.misc.Cleaner cleaner = ((sun.nio.ch.DirectBuffer) buffer).cleaner();
	cleaner.clean();
    }

    public void readWithMmap(RandomAccessFile file, EdgeHandler handler) throws Exception {
	readWithMmap(file, 1024 * 1024 * 1000l, handler);
    }

    public void readWithMmap(final RandomAccessFile file, long chunkSize, final EdgeHandler handler) throws Exception {
	long start = System.currentTimeMillis();

	final long fileSize = file.length();
	final int nChunks = (int) Math.ceil( ((double)fileSize) / chunkSize);

	final CountDownLatch latch = new CountDownLatch(nChunks);

	for (int i = 0; i < nChunks; i++) {
	    final int chunk = i;
	    final long byteStart = chunk * chunkSize;
	    final long nBytes = Math.min(fileSize, byteStart + chunkSize) - byteStart;

	    executor1.submit(new Runnable() {
		    public void run() {
			int currentBufIndex = 0;
			final int[] buf = new int[EDGES_TO_BUFFER * 2];
			final int[] n = new int[2];

			try {
			    //System.err.println("CHUNK " + chunk + " " + byteStart + " " + nBytes + " " + fileSize + " " + nChunks);
			    
			    final MappedByteBuffer buffer = file.getChannel().map(java.nio.channels.FileChannel.MapMode.READ_ONLY, byteStart, nBytes);
			    
			    try {
				//executor2.submit(new Runnable() { public void run() { buffer.load(); } });
				
				boolean ignoreLine = false;
				boolean eatingWhitespace = false;
				int x = 0;
				for (int j = 0; j < nBytes; j++) {
				    byte c = buffer.get();
				    
				    switch (c) {
				    case LF: case CR:
					if (!ignoreLine) {
					    // System.out.println(n[0] + " " + n[1]);
					    buf[currentBufIndex] = n[0];
					    buf[currentBufIndex + 1] = n[1];
					    currentBufIndex += 2;
					    if (currentBufIndex >= buf.length) {
						handler.addEdges(buf, currentBufIndex >>> 1);
						currentBufIndex = 0;
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

				if (currentBufIndex > 0) {
				    handler.addEdges(buf, currentBufIndex >>> 1);
				}
				
			    } catch (Exception e) {
				e.printStackTrace();
			    } finally {
				unmap(buffer);
			    }
			} catch (Exception e) {
			    e.printStackTrace();
			} finally {
			    System.err.println("DONE " + chunk + " " + byteStart + " " + nBytes + " " + fileSize + " " + nChunks);
			}

			latch.countDown();
		    }
		});
	}

	latch.await();
	long end = System.currentTimeMillis();
	System.err.println("ALL_DONE in " + (end-start) + " milliseconds");
    }

    static public final class NullEdgeHandler implements EdgeHandler {
	public void addEdges(int[] edges, int nEdges) {
	}
    }

    static public void main(String[] args) throws Exception {
	try {
	    new MMapReader().readWithMmap(new RandomAccessFile(args[0], "r"), 1024 * 1024 * 1000l, new NullEdgeHandler());

	} catch (Exception e) {
	    e.printStackTrace();
	} finally {
	    executor1.shutdownNow();
	}
    }
}
