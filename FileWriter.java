import java.io.*;
import java.util.concurrent.ArrayBlockingQueue;


public class FileWriter implements Runnable {

    private String downloadPath;
    private long fileSize;
    private ArrayBlockingQueue<DataChunk> blockingQueue;
    private Boolean[] metadataArray;
    private double downloadedPercentage;
    private int downloadedChunkCounter;

    public FileWriter(String fileName, long fileSize, ArrayBlockingQueue<DataChunk> blockingQueue, Boolean[] metadataArray,
                      double downloadedPercentage, int downloadedChunkCounter) {
        this.downloadPath = fileName;
        this.fileSize = fileSize;
        this.blockingQueue = blockingQueue;
        this.metadataArray = metadataArray;
        this.downloadedPercentage = downloadedPercentage;
        this.downloadedChunkCounter = downloadedChunkCounter;

    }

    public void run() {
        try {
            RandomAccessFile writer = new RandomAccessFile(this.downloadPath, "rw");
            while (this.downloadedChunkCounter < this.metadataArray.length ) {
                DataChunk currentChunk = WriteChunkToDisk(writer);
                UpdatePercentage(currentChunk.readAmount);
                UpdateMetadata(currentChunk);
                SerializeMetadata();
            }
            closeAndDeleteMetadata(writer);
        } catch (IOException message) {
            System.err.println("unable to access disk location");
            System.exit(-1);
        }
    }

    private void SerializeMetadata(){
        try {
            //serialize temp metadata
            FileOutputStream fileOutputStream = new FileOutputStream(this.downloadPath + ".meta.temp");
            ObjectOutputStream objectOutputStream = new ObjectOutputStream(fileOutputStream);
            objectOutputStream.writeObject(this.metadataArray);
            objectOutputStream.close();
            fileOutputStream.close();

            //keep the new
            File oldMetadata = new File(this.downloadPath + ".meta");
            File newMetadata = new File(this.downloadPath + ".meta.temp");
            oldMetadata.delete();
            newMetadata.renameTo(oldMetadata);
        } catch (IOException message){
            System.err.println("Serialization error");
            System.exit(-1);
        }
    }

    private void UpdateMetadata(DataChunk currentChunk){
        this.metadataArray[currentChunk.chunkIndex] = true;
        this.downloadedChunkCounter++;
    }

    private DataChunk WriteChunkToDisk(RandomAccessFile writer){
        try {
            DataChunk currentChunk = this.blockingQueue.take();
            writer.seek(currentChunk.startIndex);
            writer.write(currentChunk.data, 0, currentChunk.readAmount);
            return currentChunk;
        } catch (IOException | InterruptedException message) {
            System.err.println("Unable to write to disk");
            System.exit(-1);
        }
        return null;
    }

    private void UpdatePercentage(int readAmount){

        int prevPercentage = (int) this.downloadedPercentage;
        this.downloadedPercentage = ((((double) readAmount) * (double) 100) / ((double) this.fileSize)) + this.downloadedPercentage;
        if (prevPercentage != (int) this.downloadedPercentage) {
            System.out.println("Downloaded " + Integer.toString((int) this.downloadedPercentage) + "%");
        }
    }

    private void closeAndDeleteMetadata(RandomAccessFile writer) {
        try {
            writer.close();
            System.out.println("Downloaded 100%\nDownload succeeded");
            File oldMetadata = new File(this.downloadPath + ".meta");
            oldMetadata.delete();
        } catch (IOException message) {
            System.err.println("error during metadata update");
            System.exit(-1);
        }
    }
}



