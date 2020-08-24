import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class IdcDm {
    public static int CHUNK_SIZE = 2*4096;

    public static void main(String [] args) {
        if (args.length == 0) {
            System.out.println("usage: \n\t java IdcDm URL|URL-LIST-FILE [MAX-CONCURRENT-CONNECTIONS]");
            System.exit(0);
        }
        //get args
        ArrayList<String> urlList = extractUrls(args[0]);
        int maxThreads = (args.length > 1) ? (Integer.parseInt(args[1])) : 1;

        //make head req to get file size
         int fileSize = getFileSize(urlList.get(0));
        String fileName = getFileName(urlList.get(0));

        Boolean[] metadataArray = doesMetadataExist(fileName) ? getMetadata(fileName, fileSize) : (createNewMetadata(fileSize));
        int downloadedChunkCounter = calcChunkCounter(metadataArray);
        double downloadedPercentage = calcPercentage(downloadedChunkCounter, fileSize);
        int threadsAmount = calcNumOfThreads(maxThreads, downloadedChunkCounter * CHUNK_SIZE, fileSize);

        //create and insert new jobs
        Queue<int[]> jobQueue = new ArrayDeque<>();
        findJobsFromMetadata(metadataArray, fileSize, jobQueue, threadsAmount);
        ArrayBlockingQueue<DataChunk> blockingQueue = new ArrayBlockingQueue<>(fileSize/CHUNK_SIZE);

        String startMessage = (threadsAmount == 1) ? "Downloading..." : ("Downloading using " + threadsAmount + " connections...");
        System.out.println(startMessage);

        startWriter(fileName, fileSize, blockingQueue, metadataArray, downloadedPercentage, downloadedChunkCounter);
        startGetters(jobQueue, urlList, threadsAmount, blockingQueue);
    }

    private static boolean isUrlList(String  str_url) {
        return !str_url.startsWith("http");
    }

    private static ArrayList<String> extractUrls(String str_url){
        ArrayList<String> urlList = new ArrayList<>();
        try {
            if (isUrlList(str_url)) {
                BufferedReader listReader = new BufferedReader(new FileReader(str_url));
                String line = listReader.readLine();
                while (line != null) {
                    urlList.add(line);
                    line = listReader.readLine();
                }
                listReader.close();
            } else {
                urlList.add(str_url);
            }
            return urlList;
        } catch (IOException message) {
            System.err.println("Unable to access URL / URL list");
            System.exit(-1);
        }
        return urlList;
    }

    private static void addJob(Queue<int[]> jobQueue, int start, int end, int fileSize){
        if ((end*CHUNK_SIZE) >= fileSize) {
            int jobIndices[] = {start * CHUNK_SIZE, (fileSize - 1)};
            jobQueue.add(jobIndices);
        } else {
            int jobIndices[] = {start * CHUNK_SIZE, end * CHUNK_SIZE - 1};
            jobQueue.add(jobIndices);
        }
    }

    private static int getFileSize(String str_url){
        try {
            URL url = new URL(str_url);
            HttpURLConnection httpConnection = (HttpURLConnection) url.openConnection();
            httpConnection.setRequestMethod("HEAD");
            int fileSize = httpConnection.getContentLength();
            if(fileSize < 0){
                System.err.println("Could not connect to server");
                System.exit(-1);
            }
            return fileSize;
        } catch (IOException message) {
            System.err.println("Malformed URL");
            System.exit(-1);
        }
        return 0;
    }

    private static Boolean[] getMetadata(String fileName, int fileSize) {
        try {
            ObjectInputStream objectInputStream = new ObjectInputStream(new FileInputStream(fileName + ".meta"));
            Boolean[] meta = (Boolean[]) objectInputStream.readObject();
            objectInputStream.close();
            return meta;
        } catch (IOException | ClassNotFoundException message) {
            System.err.println("file inaccessible or not found");
            System.exit(-1);
        }
        return createNewMetadata(fileSize);
    }

    private static int calcChunkCounter(Boolean[] metadataArray) {
        int downloadedChunkCounter = 0;
        for (Boolean aMetadataArray : metadataArray) {
            if (aMetadataArray != null) {
                downloadedChunkCounter++;
            }
        }
        return downloadedChunkCounter;
    }

    private static String getFileName(String str_url) {
        return str_url.substring(str_url.lastIndexOf('/')+1);
    }

    private static double calcPercentage(double downloadedChunkCounter, long fileSize){
        return (downloadedChunkCounter * CHUNK_SIZE * 100) / fileSize;
    }

    private static Boolean[] createNewMetadata(int fileSize) {
        int totalChunksCount = (int) Math.ceil((double) fileSize / CHUNK_SIZE);
        return new Boolean[totalChunksCount];
    }

    private static boolean doesMetadataExist(String fileName) {
        File flag = new File(fileName + ".meta");
        return flag.exists();
    }

    private static int calcNumOfThreads(int maxThreads, int downloadedSize, int fileSize) {
        int threadsAmount = 1;
        if (fileSize - downloadedSize <= IdcDm.CHUNK_SIZE) {
            return threadsAmount;
        } else {
            return maxThreads;
        }
    }

    private static void divideAndAddJobs(int firstChunk, int lastChunk, int fileSize, Queue<int[]> jobQueue, int maxJobSize){
        int currStart = firstChunk;
        while (currStart < lastChunk) {
            int end = Math.min(currStart + maxJobSize, lastChunk);
            addJob(jobQueue, currStart, end, fileSize);
            currStart = end;
        }
    }

    private static void findJobsFromMetadata(Boolean[] metadataArray, int fileSize, Queue<int[]> jobQueue, int threadsAmount) {
        int i;
        int firstChunk;
        int lastChunk = 0;
        boolean newComponent = false;
        int maxJobSize = (fileSize / (CHUNK_SIZE * threadsAmount)); //chunk units

        for (i = 0; i < metadataArray.length; i++) {
            firstChunk = i;
            while ((i < metadataArray.length) && (metadataArray[i] ==  null)) {
                newComponent = true;
                lastChunk = ++i;
            }
            if (newComponent && threadsAmount == 1) {
                addJob(jobQueue, firstChunk, lastChunk, fileSize);
            } else {
                divideAndAddJobs(firstChunk, lastChunk, fileSize, jobQueue, maxJobSize);
                newComponent = false;
            }
        }
    }

    private static void startGetters(Queue<int[]> jobQueue, ArrayList<String> urlList, int threadsAmount, ArrayBlockingQueue<DataChunk> blockingQueue) {
        ExecutorService getterThreadPool = Executors.newFixedThreadPool(threadsAmount);
        int[] range;
        int urlIndex = 0;
        try {
            while((range = jobQueue.poll()) != null){
                getterThreadPool.execute(new RangeGetter(urlList.get(urlIndex), range[0],range[1], blockingQueue));
                urlIndex = (urlIndex + 1) % urlList.size();
            }
        }catch (Exception e){
            System.err.println(e);
            System.exit(-1);
        }
        getterThreadPool.shutdown();
    }

    private static void startWriter(String fileName, int fileSize, ArrayBlockingQueue<DataChunk> blockingQueue, Boolean[] metadataArray, double downloadedPercentage, int downloadedChunkCounter){
        Thread fileWriter = new Thread(new FileWriter(fileName, fileSize, blockingQueue, metadataArray, downloadedPercentage, downloadedChunkCounter));
        fileWriter.start();
    }

}
