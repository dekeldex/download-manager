

public class DataChunk {

    byte[] data;
    long startIndex;
    int readAmount;
    int chunkIndex;

    public DataChunk(byte[] data, long startIndex, int readAmount){
        this.data = data.clone();
        this.startIndex = startIndex;
        this.readAmount = readAmount;
        this.chunkIndex = (int)Math.ceil((double)startIndex/(double)IdcDm.CHUNK_SIZE);

    }
}
