package com.netflix.sunjeetsonboardingroot;

import static com.netflix.hollow.core.read.iterator.HollowOrdinalIterator.NO_MORE_ORDINALS;
import static com.netflix.sunjeetsonboardingroot.resource.v1.ConsumeAndGenerateResource.TEST_FILE_FEATHER_OVERRIDE;
import static com.netflix.sunjeetsonboardingroot.resource.v1.ConsumeAndGenerateResource.TEST_FILE_TOPN;

import com.netflix.cinder.consumer.NFHollowBlobRetriever;
import com.netflix.gutenberg.consumer.GutenbergFileConsumer;
import com.netflix.hollow.api.consumer.HollowConsumer;
import com.netflix.hollow.core.memory.encoding.BlobByteBuffer;
import com.netflix.hollow.core.read.dataaccess.HollowSetTypeDataAccess;
import com.netflix.hollow.core.read.engine.HollowBlobReader;
import com.netflix.hollow.core.read.engine.HollowReadStateEngine;
import com.netflix.hollow.core.read.iterator.HollowOrdinalIterator;
import com.netflix.sunjeetsonboardingroot.resource.v1.ConsumeFromGeneratedFilesResource;
import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.LongBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import org.junit.Test;

public class ConsumeFromGeneratedFilesResourceTest {

    @Test
    public void testReferenceReadState() throws IOException {

        HollowReadStateEngine readState = ConsumeFromGeneratedFilesResource.initReadState(TEST_FILE_TOPN);
        ConsumeFromGeneratedFilesResource.referenceReadState(readState);
    }

    @Test
    public void testSetIteration() throws IOException {

        int iter_count = 0;

        HollowReadStateEngine blobFileReadStateEngine = new HollowReadStateEngine();
        // HollowBlobFileReader is a helper that loads references from a RandomAccessFile into an in-memory HollowReadFileEngine
        HollowBlobReader fileReader = new HollowBlobReader(blobFileReadStateEngine);
        RandomAccessFile raf = new RandomAccessFile(TEST_FILE_TOPN, "r");
        FileChannel channel = raf.getChannel(); // Map MappedByteBuffer once, pass it everywhere
        BlobByteBuffer buffer = BlobByteBuffer.mmapBlob(channel);
        BufferedWriter debug = new BufferedWriter(new FileWriter("/tmp/debug_new"));

        fileReader.readSnapshot(raf, buffer, debug);

        debug.flush();

        HollowSetTypeDataAccess typeDataAccess = (HollowSetTypeDataAccess) blobFileReadStateEngine.getTypeDataAccess("SetOfTopNAttribute");

        int ordinal = 0;
        HollowOrdinalIterator iter = typeDataAccess.ordinalIterator(ordinal);

        int elementOrdinal = iter.next();

        while(elementOrdinal != NO_MORE_ORDINALS) {
            iter_count ++;
            elementOrdinal = iter.next();
        }
    }


    @Test
    public void testBufferOps() throws Exception {
        RandomAccessFile raf = new RandomAccessFile(TEST_FILE_FEATHER_OVERRIDE, "r");
        System.out.println("First 10 bytes from raf=");
        System.out.println("0 0 4 6 255 255 255 255 255 255 \n\n");

        FileChannel channel = raf.getChannel();
        MappedByteBuffer buffer1 = channel.map(FileChannel.MapMode.READ_ONLY, raf.getFilePointer(), 100);

        LongBuffer buffer2 = buffer1.asLongBuffer();
        System.out.println("buffer2.position()= " + buffer2.position());


        System.out.println("buffer2.get(3)= " + buffer2.get(3));

        System.out.println("buffer2.position()= " + buffer2.position());


//        buffer1.position(2);
//        ByteBuffer buffer2 = (ByteBuffer) buffer1.duplicate();
//        System.out.println("buffer2.position()= " + buffer2.position());
//
//        // SNAP: THIS IS THE IMPORTANT LINE
//        System.out.println("buffer2.get(1)= " + buffer2.get(buffer2.position() + 1));
//
//        buffer1.position(4);
//        System.out.println("buffer1.position()= " + buffer1.position());
//        System.out.println("buffer2.position()= " + buffer2.position());

//        buffer = (MappedByteBuffer) buffer.position(3); // returns a new direct buffer, starting at given position, with separate tracking of position
//
//        System.out.println("Final buffer pos= " + buffer.position());
//
//        System.out.println(buffer.get() + ", " + buffer.get() + ", " + buffer.get());
//         System.out.println("Buffer pos after 3 get bytes= " + buffer.position());
//
//         LongBuffer buffer2 = buffer.asLongBuffer();
//         System.out.println("buffer2 assigned to buffer, initial pos of buffer2 = " + buffer2.position()
//         + ", data at buffer2[" + buffer2.position() + "]= " + buffer2.get());
//
//         System.out.println("Data at buffer[" + buffer.position() + "]= " + buffer.get());
//
//         buffer2.get(); buffer2.get(); buffer2.get();
//
//         System.out.println("After 3 byte gets on buffer2, buffer pos= " + buffer.position()
//                 + ", data at buffer[" + buffer.position() + "]= " + buffer.get());
//
    }

    @Test
    public void testMappedByteBufferOps2() throws Exception {

        // learning how to use channel.map params
        RandomAccessFile raf = new RandomAccessFile(TEST_FILE_FEATHER_OVERRIDE, "r");
        System.out.println("First 10 bytes from raf=");
        System.out.println("0 0 4 6 255 255 255 255 255 255 \n\n");

        // Advance raf by 3 bytes
        raf.read(); raf.read(); raf.read();

        FileChannel channel = raf.getChannel();
        MappedByteBuffer buffer = channel.map(FileChannel.MapMode.READ_ONLY, raf.getFilePointer(), 7);

        System.out.println("First byte in buffer= " + buffer.get());
    }

    // Outcome: MappedByteBuffer and RandomAccessFile positional pointers are independent of each other,
    //          so they have to be advanced separately
    @Test
    public void testMappedByteBufferOps1() throws Exception {

        RandomAccessFile raf = new RandomAccessFile(TEST_FILE_FEATHER_OVERRIDE, "r");
        FileChannel channel = raf.getChannel();
        MappedByteBuffer buffer = channel.map(FileChannel.MapMode.READ_ONLY, 0, 20);

//        System.out.println("First 10 bytes from raf= ");
//        for (int i=0; i<10; i++ )
//        {
//            System.out.print(raf.read() + " ");
//        }

        System.out.println("First 10 bytes from raf=");
        System.out.println("0 0 4 6 255 255 255 255 255 255 \n\n");
        System.out.println("Starting to read file from beginning");

        System.out.println("1 byte from raf= " + raf.read());
        System.out.println("Advancing mapped byte buffer by 3 bytes ");
        buffer.position(buffer.position() + 3);
        System.out.println("1 byte from buffer= " + buffer.get());

        System.out.println("1 byte from raf= " + raf.read());
        buffer.position(buffer.position() + 3);
        System.out.println("1 byte from buffer= " + buffer.get());
        System.out.println("1 byte from raf= " + raf.read());

    }

    // @Test
    public void compareSchemasAcrossNamespaceVersions() throws IOException {
        GutenbergFileConsumer gutenberg = GutenbergFileConsumer.localProxyForProdEnvironment();
        // HollowConsumer consumerNew = HollowConsumer.withBlobRetriever(new NFHollowBlobRetriever(gutenberg, "ocp-throttling-v1-canary")).build();
        // consumerNew.triggerRefresh();
        // System.out.println("New schema version= " + consumerNew.getCurrentVersionId());

        // FileOutputStream newSchemaFile = new FileOutputStream("/tmp/newschema");
        // String newSchema = consumerNew.getStateEngine().getSchemas().toString();
        // newSchemaFile.write(newSchema.getBytes());
        // newSchemaFile.flush();
        // newSchemaFile.close();

        HollowConsumer consumerOld = HollowConsumer.withBlobRetriever(new NFHollowBlobRetriever(gutenberg, "ocp-throttling-v1-canary")).build();
        consumerOld.triggerRefreshTo(20200205213638003l);
        System.out.println("Old schema version= " + consumerOld.getCurrentVersionId());
        FileOutputStream oldSchemaFile = new FileOutputStream("/tmp/oldschema");
        String oldSchema = consumerOld.getStateEngine().getSchemas().toString();
        oldSchemaFile.write(oldSchema.getBytes());
        oldSchemaFile.flush();
        oldSchemaFile.close();
    }
}
