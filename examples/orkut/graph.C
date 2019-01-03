#include <iostream>
#include "unionFindLib.h"
#include "graph.decl.h"
#include "graph-io.h"


/*readonly*/ CProxy_UnionFindLib libProxy;
/*readonly*/ CProxy_Main mainProxy;
/*readonly*/ int64_t num_vertices;
/*readonly*/ int64_t num_edges;
/*readonly*/ int64_t num_treepieces;
/*readonly*/ int64_t num_local_vertices;

class Main : public CBase_Main {
    CProxy_TreePiece tpProxy;
    double startTime;
    public:
    Main(CkArgMsg *m) {
        if (m->argc != 3) {
            CkPrintf("Usage: ./graph <input_file>\n");
            CkExit();
        }
        std::string inputFileName(m->argv[1]);
        FILE *fp = fopen(inputFileName.c_str(), "r");
        char line[256];
        fgets(line, sizeof(line), fp);
        line[strcspn(line, "\n")] = 0;

        std::vector<std::string> params;
        split(line, ' ', &params);

        if (params.size() != 3) {
            CkAbort("Insufficient number of params provided in .g file\n");
        }

        num_vertices = std::stoi(params[0].substr(strlen("Vertices:")));
        num_edges = std::stoi(params[1].substr(strlen("Edges:")));
        //num_treepieces = std::stoi(params[2].substr(strlen("Treepieces:")));
        num_treepieces = CkNumPes();
        num_local_vertices = num_vertices / num_treepieces;

        fclose(fp);

        if (num_vertices < num_treepieces) {
            CkPrintf("Fewer vertices than treepieces\n");
            CkExit();
        }
        // TODO: need to remove passing tpProxy
        libProxy = UnionFindLib::unionFindInit(tpProxy, num_treepieces);
        CkCallback cb(CkIndex_Main::done(), thisProxy);
        libProxy[0].register_phase_one_cb(cb);
        tpProxy = CProxy_TreePiece::ckNew(inputFileName, num_treepieces);
        // find first vertex ID on last chare
        // create a callback for library to inform application after
        // completing inverted tree construction
    }

    void startWork() {
        CkPrintf("[Main] Library array with %d chares created and proxy obtained\n", num_treepieces);
        startTime = CkWallTimer();
        tpProxy.doWork();
    }

    void done() {
        CkPrintf("[Main] Inverted trees constructed. Notify library to perform components detection\n");
        CkPrintf("[Main] Tree construction time: %f\n", CkWallTimer()-startTime);
        // callback for library to inform application after completing
        // connected components detection
        CkCallback cb(CkIndex_Main::doneFindComponents(), thisProxy);
        //CkCallback cb(CkIndex_TreePiece::requestVertices(), tpProxy); //tmp, for debugging
        startTime = CkWallTimer();
        libProxy.find_components(cb);
    }

    void doneFindComponents() {
        CkPrintf("[Main] Components identified, prune unecessary ones now\n");
        CkPrintf("[Main] Components detection time: %f\n", CkWallTimer()-startTime);
        // callback for library to report to after pruning
        CkCallback cb(CkIndex_TreePiece::requestVertices(), tpProxy);
        libProxy.prune_components(1, cb);
    }

    void donePrinting() {
        CkPrintf("[Main] Final runtime: %f\n", CkWallTimer()-startTime);
        CkExit();
    }

};

class TreePiece : public CBase_TreePiece {

    std::vector<proteinVertex> myVertices;
    std::vector< std::pair<int64_t, int64_t> > library_requests;
    int64_t numMyVertices;
    int64_t numMyEdges;
    int64_t myID;
    FILE *input_file;
    UnionFindLib *libPtr;
    unionFindVertex *libVertices;

    public:
    // function that must be always defined by application
    // return type -> std::pair<int, int>
    // this specific logic assumes equal distribution of vertices across all tps
    static std::pair<int64_t, int64_t> getLocationFromID(int64_t vid);
    
    TreePiece(std::string filename) {
        input_file = fopen(filename.c_str(), "r");
        myID = CkMyPe();
        numMyVertices = num_vertices / num_treepieces;
        if (myID == (num_treepieces - 1)) {
          numMyVertices += num_vertices % num_treepieces;
        }
        libPtr = libProxy[thisIndex].ckLocal();
        // only 1 chare in PE (group)
        libPtr->allocate_libVertices(numMyVertices, 1);

        /*numMyVertices = num_vertices / num_treepieces;
        if (thisIndex == num_treepieces - 1) {
            // last chare should get all remaining vertices if not equal division
            numMyVertices += num_vertices % num_treepieces;
        }
        myVertices = new proteinVertex[numMyVertices];*/
        // populate myVertices
        //populateMyVertices(myVertices, num_vertices, num_treepieces, thisIndex, input_file);
        //numMyVertices = myVertices.size();
        /*if (thisIndex == 0) {
            for (int i = 0; i < numMyVertices; i++)
                CkPrintf("myVertices[%d] = id: %ld\n", i, myVertices[i].id);
        }*/
        libPtr->initialize_vertices(numMyVertices, libVertices, 0 /*offset*/, 999999999 /*batchSize - need to turn off*/);
        int64_t offset = myID * (num_vertices / num_treepieces); /*the last PE might have different number of vertices*/;
        for (int64_t i = 0; i < numMyVertices; i++) {
          libVertices[i].vertexID = libVertices[i].parent = offset + i;
        }


        // reset input_file pointer
        fseek(input_file, 0, SEEK_SET);
        numMyEdges = num_edges / num_treepieces;
        if (myID == (num_treepieces - 1)) {
            // last chare should get all remaining edges if not equal division
            numMyEdges += num_edges % num_treepieces;
        }
        populateMyEdges(&library_requests, numMyEdges, (num_edges/num_treepieces), thisIndex, input_file, num_vertices);
        libPtr->registerGetLocationFromID(getLocationFromID);
        contribute(CkCallback(CkReductionTarget(Main, startWork), mainProxy));
    }

    TreePiece(CkMigrateMessage *msg) { }



    void doWork() {
        // vertices and edges populated, now fire union requests
        for (int i = 0; i < library_requests.size(); i++) {
            std::pair<int64_t, int64_t> req = library_requests[i];
            libPtr->union_request(req.first, req.second);
        }
    }

    void requestVertices() {
        unionFindVertex *finalVertices = libPtr->return_vertices();
        for (int i = 0; i < numMyVertices; i++) {
            //CkPrintf("[tp%d] myVertices[%d] - vertexID: %ld, parent: %ld, component: %d\n", thisIndex, i, finalVertices[i].vertexID, finalVertices[i].parent, finalVertices[i].componentNumber);
#ifndef ANCHOR_ALGO
            if (finalVertices[i].parent != -1 && finalVertices[i].componentNumber == -1) {
#else
            if (finalVertices[i].parent != finalVertices[i].vertexID && finalVertices[i].componentNumber == -1) {
#endif
                CkAbort("Something wrong in inverted-tree construction!\n");
            }
        }
        contribute(CkCallback(CkReductionTarget(Main, donePrinting), mainProxy));
    }

    void getConnectedComponents() {
        //libPtr->find_components();
        for (int i = 0; i < numMyVertices; i++) {
            CkPrintf("[tp%d] myVertices[%d] - vertexID: %ld, parent: %ld, component: %d\n", thisIndex, i, libVertices[i].vertexID, libVertices[i].parent, libVertices[i].componentNumber);
        }
    }

    void test() {
        CkPrintf("It works!\n");
        CkExit();
    }
};

/*
std::pair<int, int>
TreePiece::getLocationFromID(long int vid) {
    int chareIdx = (vid-1) / (num_vertices/num_treepieces);
    chareIdx = std::min(chareIdx, num_treepieces-1);
    int arrIdx;
    if (vid > lastChareBegin)
        arrIdx = vid - lastChareBegin;
    else
        arrIdx = (vid-1) % (num_vertices/num_treepieces);
    return std::make_pair(chareIdx, arrIdx);
}
*/

std::pair<int64_t, int64_t>
TreePiece::getLocationFromID(int64_t vid) {
  int64_t chareIdx = vid / num_treepieces;
  int64_t arrIdx = vid % num_treepieces;
  if (chareIdx == num_treepieces) {
    chareIdx--;
    if (vid >= num_local_vertices * num_treepieces) {
      arrIdx += num_local_vertices;
    }
  }
  return std::make_pair(chareIdx, arrIdx);
}


#include "graph.def.h"
