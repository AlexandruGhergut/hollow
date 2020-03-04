package com.netflix.sunjeetsonboardingroot.resource.v1;

import static com.netflix.sunjeetsonboardingroot.resource.v1.ConsumeAndGenerateResource.TEST_FILE_FEATHER;
import static com.netflix.sunjeetsonboardingroot.resource.v1.ConsumeAndGenerateResource.TEST_FILE_FEATHER_OVERRIDE;
import static com.netflix.sunjeetsonboardingroot.resource.v1.ConsumeAndGenerateResource.TEST_FILE_TOPN;

import com.netflix.hollow.api.objects.delegate.HollowObjectGenericDelegate;
import com.netflix.hollow.api.objects.generic.GenericHollowObject;
import com.netflix.hollow.core.index.key.PrimaryKey;
import com.netflix.hollow.core.memory.encoding.BlobByteBuffer;
import com.netflix.hollow.core.read.dataaccess.HollowDataAccess;
import com.netflix.hollow.core.read.engine.HollowBlobReader;
import com.netflix.hollow.core.read.engine.HollowReadStateEngine;
import com.netflix.hollow.core.read.engine.object.HollowObjectTypeReadState;
import com.netflix.hollow.explorer.ui.jetty.HollowExplorerUIServer;
import com.netflix.sunjeetsonboardingroot.generated.topn.TopNAPI;
import com.netflix.sunjeetsonboardingroot.startup.JerseyModule;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.BitSet;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

@Path(JerseyModule.CONSUME_FROM_GENERATED_PATH + "{path: (/.*)?}")
public class ConsumeFromGeneratedFilesResource {

    @GET
    @Produces(MediaType.TEXT_HTML)
    public Response queryConsume() throws Exception {

        HollowReadStateEngine readState = initReadState(TEST_FILE_FEATHER);

        // Explore
        HollowExplorerUIServer uiServer = new HollowExplorerUIServer(readState, 7777);
        uiServer.start();
        uiServer.join();

        return Response.ok().build();
    }

    public static HollowReadStateEngine initReadState(String file) throws IOException {
        // vanilla impl
        // HollowReadStateEngine readState = new HollowReadStateEngine(true);
        // HollowBlobReader reader = new HollowBlobReader(readState);
        // reader.readSnapshot(new BufferedInputStream(new FileInputStream(file)));

        // RandomAccessFile impl
        HollowReadStateEngine blobFileReadStateEngine = new HollowReadStateEngine();
        // HollowBlobFileReader is a helper that loads references from a RandomAccessFile into an in-memory HollowReadFileEngine
        HollowBlobReader fileReader = new HollowBlobReader(blobFileReadStateEngine);
        RandomAccessFile raf = new RandomAccessFile(file, "r");
        FileChannel channel = raf.getChannel(); // Map MappedByteBuffer once, pass it everywhere
        System.out.println("SNAP: mmapping start");
        BlobByteBuffer buffer = BlobByteBuffer.mmapBlob(channel);
        System.out.println("SNAP: mmapping end");
        BufferedWriter debug = new BufferedWriter(new FileWriter("/tmp/debug_new"));

        System.out.println("SNAP: start reading snapshot");
        fileReader.readSnapshot(raf, buffer, debug);
        System.out.println("SNAP: end reading snapshot");

        debug.flush();

        return blobFileReadStateEngine;
    }

    public static HollowReadStateEngine referenceReadState(HollowReadStateEngine blobFileReadStateEngine) throws IOException {

//        HollowObjectTypeReadState stringTypeState = (HollowObjectTypeReadState) blobFileReadStateEngine.getTypeState("String");
//        HollowDataAccess typeDataAccess = stringTypeState.getDataAccess();
//        BitSet typePopulatedOrdinals = stringTypeState.getPopulatedOrdinals();
//        int typeOrdinal = typePopulatedOrdinals.nextSetBit(0);
//        int typeCount = 0;
//
//        while (typeOrdinal != -1 && typeCount < 5) {
//            GenericHollowObject obj = new GenericHollowObject(new HollowObjectGenericDelegate(stringTypeState), typeOrdinal);
//            System.out.println("obj= " + obj);
//            typeOrdinal = typePopulatedOrdinals.nextSetBit(typeOrdinal + 1);
//            typeCount ++;
//        }

         TopNAPI topNAPI = new TopNAPI(blobFileReadStateEngine);
         topNAPI.getAllTopN();

         HollowObjectTypeReadState typeState = (HollowObjectTypeReadState) blobFileReadStateEngine.getTypeState("TopN");

         BitSet populatedOrdinals = typeState.getPopulatedOrdinals();
         PrimaryKey pkey = typeState.getSchema().getPrimaryKey();
         int numOrdinals = populatedOrdinals.cardinality();
         System.out.println("Ordinal cardinality= " + numOrdinals);

         int ordinal = populatedOrdinals.nextSetBit(0);
         int count = 0;
         while (ordinal != -1 && count < 1) {
             GenericHollowObject obj = new GenericHollowObject(new HollowObjectGenericDelegate(typeState), ordinal);
             System.out.println("ordinal= " + ordinal);

             long id = obj.getLong("videoId");

             System.out.println("id= " + id);
             System.out.println("obj= " + obj);

             ordinal = populatedOrdinals.nextSetBit(ordinal + 1);
             count ++;
         }

        return blobFileReadStateEngine;
    }
}
