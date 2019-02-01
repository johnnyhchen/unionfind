#include <assert.h>
#include <iostream>
#include <math.h>
#include "unionFindLib.h"
#include "graph.decl.h"
#include "graph-io.h"


/*readonly*/ CProxy_UnionFindLib libProxy;
/*readonly*/ CProxy_Main mainProxy;
/*readonly*/ int64_t num_vertices;
/*readonly*/ int64_t num_edges;
/*readonly*/ int64_t num_treepieces;
/*readonly*/ int64_t num_local_vertices;
/*readonly*/ int64_t edge_batch_size;

class Main : public CBase_Main {
    CProxy_TreePiece tpProxy;
    double startTime;
    int64_t max_local_edges;
    int64_t ebatchNo;
    int64_t num_edge_batches;
    public:
    Main(CkArgMsg *m) {
        if (m->argc != 3) {
            CkPrintf("Usage: ./graph <input_file> <edge_batch_size> \n");
            CkExit();
        }
        // assert(0);
        std::string inputFileName(m->argv[1]);
        edge_batch_size = atol(m->argv[2]);
        FILE *fp = fopen(inputFileName.c_str(), "r");
        char line[256];
        fgets(line, sizeof(line), fp);
        fgets(line, sizeof(line), fp);
        fgets(line, sizeof(line), fp);
        line[strcspn(line, "\n")] = 0;

        std::vector<std::string> params;
        split(line, ' ', &params);

        if (params.size() != 5) {
            CkAbort("Insufficient number of params provided in .g file\n");
        }

        //num_vertices = std::stoi(params[0].substr(strlen("Nodes:")));
        //num_edges = std::stoi(params[1].substr(strlen("Edges:")));
        num_vertices = std::stoi(params[2]);
        num_edges = std::stoi(params[4]);
        CkPrintf("num_vertices: %ld num_edges: %ld\n", num_vertices, num_edges);
        // CkExit();
        //num_treepieces = std::stoi(params[2].substr(strlen("Treepieces:")));
        num_treepieces = CkNumPes();
        num_local_vertices = num_vertices / num_treepieces;
        max_local_edges = (int64_t) ceil(num_edges / num_treepieces);
        num_edge_batches = (int64_t) ceil(max_local_edges / edge_batch_size);

        fclose(fp);

        if (num_vertices < num_treepieces) {
            CkPrintf("Fewer vertices than treepieces\n");
            CkExit();
        }
        // TODO: need to remove passing tpProxy
        libProxy = UnionFindLib::unionFindInit();
        CkCallback cb(CkIndex_Main::done(), thisProxy);
        libProxy[0].register_phase_one_cb(cb);
        tpProxy = CProxy_TreePiece::ckNew(inputFileName);
        // find first vertex ID on last chare
        // create a callback for library to inform application after
        // completing inverted tree construction
    }

    void startWork() {
        CkPrintf("[Main] Library array with %d chares created and proxy obtained max_local_edges: %ld num_edge_batches: %ld\n", num_treepieces, max_local_edges, num_edge_batches);
        startTime = CkWallTimer();
        ebatchNo = 1;
        tpProxy.doWork();
    }

    void done() {
        // CkPrintf("[Main] Inverted trees constructed. Notify library to perform components detection\n");
        // CkPrintf("[Main] Tree construction time: %f\n", CkWallTimer()-startTime);
        // callback for library to inform application after completing
        // connected components detection
        CkPrintf("Done tree construction batchNo: %ld\n", ebatchNo);
        CkCallback cb(CkIndex_Main::donePrepareFindComponents(), thisProxy);
        //CkCallback cb(CkIndex_TreePiece::requestVertices(), tpProxy); //tmp, for debugging
        //startTime = CkWallTimer();
        libProxy.prepare_for_component_labeling(cb);
    }

    void donePrepareFindComponents() {
        CkPrintf("Done prepare for component labeling batchNo: %ld\n", ebatchNo);
        CkCallback cb(CkIndex_Main::doneFindComponents(), thisProxy);
        libProxy.inter_start_component_labeling(cb);
    }

    void doneFindComponents() {
      if (ebatchNo <= num_edge_batches) {
        CkPrintf("Done component labeling batchNo: %ld\n", ebatchNo);
        ebatchNo++;
        CkCallback cb(CkIndex_Main::done(), thisProxy);
        libProxy[0].register_phase_one_cb(cb);
        tpProxy.doWork();
      }
      else {
        CkPrintf("[Main] Components identified, prune unecessary ones now\n");
        CkPrintf("[Main] Tree construction + components detection time: %f\n", CkWallTimer() - startTime);
        CkExit();
        CkCallback cb(CkIndex_TreePiece::requestVertices(), tpProxy);
        libProxy.prune_components(1, cb);
      }
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
    int64_t edgesProcessed;

    public:
    // function that must be always defined by application
    // return type -> std::pair<int, int>
    // this specific logic assumes equal distribution of vertices across all tps
    static std::pair<int64_t, int64_t> getLocationFromID(int64_t vid);
    
    TreePiece(std::string filename) {
        edgesProcessed = 0;
        input_file = fopen(filename.c_str(), "r");
        myID = CkMyPe();
        numMyVertices = num_vertices / num_treepieces;
        /*
        if (myID == (num_treepieces - 1)) {
          numMyVertices += num_vertices % num_treepieces;
        }
        */
        if ((myID + 1) <= (num_vertices % num_treepieces)) {
          numMyVertices++;
        }
        libPtr = libProxy.ckLocalBranch();
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
        int64_t dummy = 0;
        libPtr->initialize_vertices(numMyVertices, libVertices, dummy /*offset*/, 999999999 /*batchSize - need to turn off*/);
        int64_t offset = myID * (num_vertices / num_treepieces); /*the last PE might have different number of vertices*/;
        int64_t startID = myID;
        for (int64_t i = 0; i < numMyVertices; i++) {
          libVertices[i].vertexID = libVertices[i].parent = startID;
          startID += num_treepieces;
          std::pair<int64_t, int64_t> w_id = getLocationFromID(libVertices[i].vertexID);
          // assert (w_id.first == myID);
          // CkPrintf("vertexID: %ld i: %ld w_id.second: %ld\n", libVertices[i].vertexID, i, w_id.second);
          assert (w_id.second == i);
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
        CkPrintf("Finished reading my part of the file: %d\n", CkMyPe());
        contribute(CkCallback(CkReductionTarget(Main, startWork), mainProxy));
    }

    TreePiece(CkMigrateMessage *msg) { }



    void doWork() {
      // vertices and edges populated, now fire union requests
      for (int64_t i = 0; edgesProcessed < library_requests.size(); edgesProcessed++, i++) {
        if (i == edge_batch_size) {
          break;
        }
        std::pair<int64_t, int64_t> req = library_requests[edgesProcessed];
        // CkPrintf("PE: %d union(%ld %ld)\n", CkMyPe(), req.first, req.second);
        libPtr->union_request(req.first, req.second);
      }
    }

    void requestVertices() {
        // unionFindVertex *finalVertices = libPtr->return_vertices();
        for (int i = 0; i < numMyVertices; i++) {
            //CkPrintf("[tp%d] myVertices[%d] - vertexID: %ld, parent: %ld, component: %d\n", thisIndex, i, finalVertices[i].vertexID, finalVertices[i].parent, finalVertices[i].componentNumber);
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
  // int64_t vRatio = num_vertices / num_treepieces;
  // CkPrintf("Req for vid: %ld num_local_vertices: %ld\n", vid, num_local_vertices);
  int64_t chareIdx = vid % num_treepieces;
  int64_t arrIdx = vid / num_treepieces;
  /*
  if (chareIdx == num_treepieces) {
    chareIdx--;
    if (vid >= num_local_vertices * num_treepieces) {
      arrIdx += num_local_vertices;
    }
  }
  */
  /*
  if (vid >= num_local_vertices * num_treepieces) {
    arrIdx = vid - (num_local_vertices * (num_treepieces - 1));
    // arrIdx += num_local_vertices;
    chareIdx = num_treepieces - 1;
  }
  */
  // CkPrintf("vid: %ld chareIdx: %ld arrIdx: %ld\n", vid, chareIdx, arrIdx);
  /*
  if (arrIdx > 768111) {
    // CkPrintf("", CkMyPe(), chareIdx, arrIdx);
    CkPrintf("PE: %d vid: %ld chareIdx: %ld arrIdx: %ld\n", CkMyPe(), vid, chareIdx, arrIdx);
    CkExit();
  }
  */
  return std::make_pair(chareIdx, arrIdx);
}


#include "graph.def.h"
