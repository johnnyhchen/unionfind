#include <assert.h>
#include <iostream>
#include <math.h>
#include "unionFindLib.h"
#include "graph.decl.h"
#include "graph-io.h"


///*readonly*/ CProxy_UnionFindLib libProxy;
/*readonly*/ CProxy_UnionFindLibCache libCacheProxy;
/*readonly*/ CProxy_Main mainProxy;
/*readonly*/ int64_t num_vertices;
/*readonly*/ int64_t num_edges;
/*readonly*/ int64_t num_treepieces;
/*readonly*/ int64_t num_local_vertices;
/*readonly*/ int64_t edge_batch_size;
/*readonly*/ int64_t num_nodes;

class Main : public CBase_Main {
    CProxy_UnionFindLib libProxy;
    CProxy_TreePiece tpProxy;
    double startTime;
    int64_t max_local_edges;
    int64_t ebatchNo;
    int64_t num_edge_batches;
    std::string inputFileName;
    int done_treeAndLabelingCalled;
    public:
    Main(CkArgMsg *m) {
        if (m->argc != 3) {
            CkPrintf("Usage: ./graph <input_file> <edge_batch_size> \n");
            CkExit();
        }
        done_treeAndLabelingCalled = 0;
        // assert(0);
        inputFileName = m->argv[1];
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
        num_nodes = CmiNumNodes();
        CkPrintf("num_nodes: %d\n", num_nodes);
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
        CkCallback cb(CkIndex_Main::doneCacheInit(), thisProxy);
        libCacheProxy = UnionFindLibCache::UnionFindLibCacheInit(cb);
        CkPrintf("PE: %d libCacheProxy.id: %d\n", CkMyPe(), libCacheProxy.ckGetGroupID().idx);
    }

    void doneCacheInit() {
        CkCallback cb(CkIndex_Main::prCrDone(), thisProxy);
        libProxy = UnionFindLib::unionFindInit(cb);
    }

    void prCrDone() {
        // CkPrintf("In prCrDone()\n");
        tpProxy = CProxy_TreePiece::ckNew(inputFileName, libProxy);
        // find first vertex ID on last chare
        // create a callback for library to inform application after
        // completing inverted tree construction
    }

    void dontInitVertices() {
      // Initialize offsets in each nodegroup
      // CkPrintf("In dontInitVertices()\n");
      CkCallback cb(CkIndex_Main::done_treeAndLabeling(), thisProxy);
      libProxy[0].register_phase_one_cb(cb);
      startWork();
      /*
      CkCallback cb1(CkIndex_Main::startWork(), thisProxy);
      libCacheProxy.initOffsets(cb1);
      */
    }

    void startWork() {
        CkPrintf("[Main] Library array with %d chares created and proxy obtained max_local_edges: %ld num_edge_batches: %ld\n", num_treepieces, max_local_edges, num_edge_batches);
        startTime = CkWallTimer();
        ebatchNo = 1;
        tpProxy.doWork();

        // start component labeling in parallel
        CkCallback cb(CkIndex_Main::donePrepareFindComponents(), thisProxy);
        libCacheProxy.prepare_for_component_labeling(cb);
    }

    void done() {
        // CkPrintf("[Main] Inverted trees constructed. Notify library to perform components detection\n");
        // CkPrintf("[Main] Tree construction time: %f\n", CkWallTimer()-startTime);
        // callback for library to inform application after completing
        // connected components detection
        // CkPrintf("Done tree construction batchNo: %ld\n", ebatchNo);
        ebatchNo++;
        if (ebatchNo <= num_edge_batches) {
          // CkPrintf("Done component labeling batchNo: %ld\n", ebatchNo);
          // CkCallback cb(CkIndex_Main::done(), thisProxy);
          // libProxy[0].register_phase_one_cb(cb);
          tpProxy.doWork();
        }
        // CkCallback cb(CkIndex_Main::donePrepareFindComponents(), thisProxy);
        // CkCallback cb(CkIndex_TreePiece::requestVertices(), tpProxy); //tmp, for debugging
        // startTime = CkWallTimer();
        // libProxy.prepare_for_component_labeling(cb);
        // tpProxy.doWork();
    }

    void donePrepareFindComponents() {
        //CkPrintf("Done prepare for component labeling batchNo: %ld\n", ebatchNo);
        CkCallback cb(CkIndex_Main::doneFindComponents(), thisProxy);
        libCacheProxy.inter_start_component_labeling(cb);
    }

    void doneFindComponents() {
      /*
      if (ebatchNo <= num_edge_batches) {
        CkPrintf("Done component labeling batchNo: %ld\n", ebatchNo);
        CkCallback cb(CkIndex_Main::done(), thisProxy);
        libProxy[0].register_phase_one_cb(cb);
        tpProxy.doWork();
      }
      else {
      */
      //}
      //
      // If I have not yet reached the required number of batches
      if (ebatchNo <= num_edge_batches) {
        CkCallback cb(CkIndex_Main::donePrepareFindComponents(), thisProxy);
        libCacheProxy.prepare_for_component_labeling(cb);
      }
      else {
        thisProxy.done_treeAndLabeling();
      }
      //CkPrintf("Done component labeling batchNo: %ld\n", ebatchNo);
    }

    void done_treeAndLabeling()
    {
      done_treeAndLabelingCalled++;
      if (done_treeAndLabelingCalled == 2) {
        CkCallback cb(CkIndex_Main::donePrepareFindComponentsFinal(), thisProxy);
        libCacheProxy.prepare_for_component_labeling(cb);
      }
      assert (done_treeAndLabelingCalled <= 2);
    }
    
    void donePrepareFindComponentsFinal() {
        //CkPrintf("Done prepare for component labeling for the final time batchNo: %ld\n", ebatchNo);
        CkCallback cb(CkIndex_Main::done_exit(), thisProxy);
        libCacheProxy.inter_start_component_labeling(cb);
    }

    void donePrinting() {
        CkPrintf("[Main] Final runtime: %f\n", CkWallTimer()-startTime);
        CkExit();
    }

    void done_exit()
    {
        //CkPrintf("[Main] Components identified, prune unecessary ones now\n");
        CkPrintf("[Main] Tree construction + components detection time: %f\n", CkWallTimer() - startTime);
        CkExit();
        CkCallback cb(CkIndex_TreePiece::requestVertices(), tpProxy);
        libProxy.prune_components(1, cb);
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
    //UnionFindLibCache *libPtrCache;
    unionFindVertex *libVertices;
    int64_t edgesProcessed;
    int64_t offset;
    int myNode;

    public:
    // function that must be always defined by application
    // return type -> std::pair<int, int>
    // this specific logic assumes equal distribution of vertices across all tps
    static std::pair<int64_t, int64_t> getLocationFromID(int64_t vid);
    
    TreePiece(std::string filename, CProxy_UnionFindLib libProxy) {
        edgesProcessed = 0;
        offset = 0;
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
        
        // myRank = CmiPhysicalRank(CkMyPe());
        myNode = CmiMyNode();
        // int *nodePeList;
        // int numpes;
        int64_t totVerticesinNode = 0;
        // CmiGetPesOnPhysicalNode(CmiPhysicalNodeID(CkMyPe()), &nodePeList, &numpes);
        std::vector<int> nodePeList;
        for (int pe = 0; pe < CkNumPes(); pe++) {
          if (CmiNodeOf(pe) == myNode) {
            nodePeList.push_back(pe);
          }
        }
        int numpes = nodePeList.size();

        for (int i = 0; i < numpes; i++) {

          totVerticesinNode += (num_vertices / num_treepieces);
          if ((nodePeList[i] + 1) <= (num_vertices % num_treepieces)) {
            totVerticesinNode++;
          }
          
          // For offset calculation
          if (nodePeList[i] < CkMyPe()) {
            offset += (num_vertices / num_treepieces);
            if ((nodePeList[i] + 1) <= (num_vertices % num_treepieces)) {
              offset++;
            }
          }
        }

        libPtr = libProxy.ckLocalBranch();
        // only the first PE in the node calls for allocation
        if (nodePeList[0] == CkMyPe()) {
          libPtr->allocate_libVertices(totVerticesinNode, 1);
        }

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
        CkPrintf("PE: %d myNode: %d offset: %ld\n", CkMyPe(), myNode, offset);
        if (CkMyPe() == 0) {
          for (int i = 0; i < numpes; i++) {
            CkPrintf("%d ", nodePeList[i]);
          }
          CkPrintf("\n\n");
        }
        // Rank-0 on this node has allocated memory for the vertices; now initialize the vertices of all the treepieces
        // if (CmiPhysicalRank(CkMyPe()) == 0) {
        // Do a contribute from all PEs to initialize their vertices
        //
        CkPrintf("PE: %d libCacheProxy: %d\n", CkMyPe(), libCacheProxy);
        CkPrintf("PE: %d libCacheProxy.id: %d\n", CkMyPe(), libCacheProxy.ckGetGroupID().idx);
        CkPrintf("PE: %d libCacheProxy.ckLocalBranch(): %d\n", CkMyPe(), libCacheProxy.ckLocalBranch());
        contribute(CkCallback(CkReductionTarget(TreePiece, init_vertices), thisProxy));
          // thisProxy.init_vertices();
        // }
    }

    void init_vertices() {
        // int64_t dummy = 0;
        libPtr->initialize_vertices(numMyVertices, libVertices, offset /*offset*/, 999999999 /*batchSize - need to turn off*/);
        // int64_t offset = myID * (num_vertices / num_treepieces); /*the last PE might have different number of vertices*/;
        CkPrintf("Finished lib:initialize_vertices(): PE: %d\n", CkMyPe());
        int64_t startID = myID;
        for (int64_t i = 0; i < numMyVertices; i++) {
          libVertices[i].vertexID = libVertices[i].parent = startID;
          startID += num_treepieces;
          // std::pair<int64_t, int64_t> w_id = getLocationFromID(libVertices[i].vertexID);
          // assert (w_id.first == myID);
          // CkPrintf("vertexID: %ld i: %ld w_id.second: %ld\n", libVertices[i].vertexID, i, w_id.second);
          // assert (w_id.second == i);
        }


        // reset input_file pointer
        fseek(input_file, 0, SEEK_SET);
        numMyEdges = num_edges / num_treepieces;
        if (myID == (num_treepieces - 1)) {
            // last chare should get all remaining edges if not equal division
            numMyEdges += num_edges % num_treepieces;
        }
        CkPrintf("PE: %d numMyEdges: %lld num_vertices: %lld\n", CkMyPe(), numMyEdges, num_vertices);
        populateMyEdges(&library_requests, numMyEdges, (num_edges/num_treepieces), thisIndex, input_file, num_vertices);
        CkPrintf("PE: %d done populateMyEdges() num_vertices: %lld\n", CkMyPe(), num_vertices);
        libPtr->registerGetLocationFromID(getLocationFromID);
        CkPrintf("Finished reading my part of the file: %d\n", CkMyPe());
        contribute(CkCallback(CkReductionTarget(Main, dontInitVertices), mainProxy));
    }

    TreePiece(CkMigrateMessage *msg) { }



    void doWork() {
      // vertices and edges populated, now fire union requests
      //CkPrintf("PE: %d started processing edges edgesProcessed: %lld library_requests.size(): %lld edge_batch_size: %lld\n", CkMyPe(), edgesProcessed, library_requests.size(), edge_batch_size);
      for (int64_t i = 0; edgesProcessed < library_requests.size(); edgesProcessed++, i++) {
        if (i == edge_batch_size) {
          //CkPrintf("Calling done() PE: %d\n", CkMyPe());
          contribute(CkCallback(CkReductionTarget(Main, done), mainProxy));
          // contribute(CkCallback(CkReductionTarget(TreePiece, doWork), thisProxy));
          break;
        }
        std::pair<int64_t, int64_t> req = library_requests[edgesProcessed];
        //CkPrintf("PE: %d union(%ld %ld) library_requests.size(): %d\n", CkMyPe(), req.first, req.second, library_requests.size());
        libPtr->union_request(req.first, req.second);
        //CkPrintf("PE: %d union(%ld %ld) request done library_requests.size(): %d\n", CkMyPe(), req.first, req.second, library_requests.size());
      }
      if (edgesProcessed == library_requests.size()) {
        //CkPrintf("Calling done() PE: %d\n", CkMyPe());
        contribute(CkCallback(CkReductionTarget(Main, done), mainProxy));
        // contribute(CkCallback(CkReductionTarget(TreePiece, doWork), thisProxy));
        // break;
      }
      //CkPrintf("PE: %d done processing all edges\n", CkMyPe());
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
  //CkPrintf("PE: %d Req for vid: %ld num_local_vertices: %ld libCacheProxy: %d\n", CkMyPe(), vid, num_local_vertices, libCacheProxy);

  //CkPrintf("PE: %d ckLocalBranch: %d\n", CkMyPe(), libCacheProxy.ckLocalBranch());
  UnionFindLibCache *libPtrCache;
  libPtrCache = libCacheProxy.ckLocalBranch();
  //CkPrintf("PE: %d Obtained pointer for libPtrCache\n", CkMyPe());
  int64_t chareIdx = vid % num_treepieces;
  int64_t arrIdx = (vid / num_treepieces);
  int64_t off = libPtrCache->get_offset(chareIdx);
  /*
  if (chareIdx != 0)
    assert (off != 0);
  */
  arrIdx += off;
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
  //CkPrintf("PE: %d vid: %ld chareIdx: %ld arrIdx: %ld\n", CkMyPe(), vid, chareIdx, arrIdx);

  return std::make_pair(chareIdx, arrIdx);
}


#include "graph.def.h"
