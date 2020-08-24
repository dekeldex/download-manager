import java.io.*;
import java.net.*;
import java.util.concurrent.ArrayBlockingQueue;


public class RangeGetter implements Runnable{
    String downloadURL;
    long rangeStart;
    long rangeEnd;
    ArrayBlockingQueue<DataChunk> blockingQueue;
    long threadID;

    public RangeGetter(String url, long startIndex, long rangeEnd, ArrayBlockingQueue<DataChunk> blockingQueue) {
        this.downloadURL = url;
        this.rangeStart = startIndex;
        this.rangeEnd = rangeEnd;
        this.blockingQueue = blockingQueue;
        this.threadID = 0;
    }

    public void run() {
        this.threadID = Thread.currentThread().getId();
        System.out.println("[" + this.threadID  + "] Start downloading range ("
                + this.rangeStart + " - " + this.rangeEnd + ") from: \n" + this.downloadURL);
        Download();
        System.out.println("[" + this.threadID  + "] Finished downloading");
    }


    private void Download(){
        try {
            URL url = new URL(this.downloadURL);
            HttpURLConnection urlConnection = (HttpURLConnection) url.openConnection();
            urlConnection.setConnectTimeout(15 * 1000); // connect timeout after 15 seconds
            urlConnection.setReadTimeout(45 * 1000); // download timeout after 45 seconds
            urlConnection.setRequestProperty("Range", "bytes="+this.rangeStart+"-"+this.rangeEnd);
            urlConnection.connect(); //Do we need this?

            BufferedInputStream in = new BufferedInputStream(urlConnection.getInputStream());
            long startIndex = this.rangeStart;
            byte data[] = new byte[IdcDm.CHUNK_SIZE];
            int readAmount;

            while ((readAmount = (in.read(data,0, IdcDm.CHUNK_SIZE))) != -1) {
                int fill = 0;
                while(readAmount < IdcDm.CHUNK_SIZE && readAmount != -1){
                    fill = (in.read(data,readAmount, IdcDm.CHUNK_SIZE - readAmount));
                    if(fill == -1){ break; }
                    readAmount += fill;
                }

                DataChunk currentChunk = new DataChunk(data, startIndex, readAmount);
                try {
                    this.blockingQueue.put(currentChunk);
                } catch (InterruptedException message) {
                    System.err.println("Could not download- Insert to blocking queue error.");
                    System.exit(-1);
                }
                startIndex += readAmount;
                if(fill == -1){
                    break;
                }
            }
        } catch(SocketTimeoutException message) {
            System.err.println("Timeout has occurred");
            System.exit(-1);
        } catch (Exception e) {
            System.err.println("Thread:[" + this.threadID +"] connection to server failed. Message: " + e.getMessage() + "\nDownload failed");
            System.exit(-1);
        }
    }
}
