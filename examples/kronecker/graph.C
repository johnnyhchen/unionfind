#include <assert.h>
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

  int desired_num_edges;
  uint8_t scale;
  uint8_t edgeFactor;
  std::vector<std::pair<int64_t, int64_t>> edgeList;

  public:
  Main(CkArgMsg *m) {
    if (m->argc != 3) {
      CkPrintf("Usage: ./graph <scale> <edgefactor>\n");
      CkExit();
    }
    scale = atoi(m->argv[1]);
    num_vertices = 1UL << scale;
    edgeFactor = atoi(m->argv[2]);
    desired_num_edges = edgeFactor * num_vertices;
    CkPrintf("num_vertices: %ld, desired_num_edges: %ld\n", num_vertices, desired_num_edges);

    num_treepieces = CkNumPes();
    if (num_vertices < num_treepieces) {
      CkPrintf("Fewer vertices than treepieces\n");
      CkExit();
    }
    num_local_vertices = num_vertices / num_treepieces;

    // Generate kronecker graph
    int64_t seeds[2] = {1, 2};
    const double initiator[4] = {.57, .19, .19, .05};
    int64_t created_num_edges;
    int64_t* edges;

    make_graph(scale, desired_num_edges, seeds[0], seeds[1], initiator, &num_edges, &edges);

    for (int i = 0; i < num_edges; i++) {
      int64_t src = edges[2*i];
      int64_t dest = edges[2*i+1];

      if ((src >= 0) && (dest >= 0)) {
        // Valid edge
        edgeList.emplace_back(src, dest);
        edgeList.emplace_back(dest, src); // Add reverse edge if mode is undirected
      }
    }

    // Free temporary memory used in graph generation
    free(edges);

    // TODO Invoke edge population separately, with respective parts of the edge list

    // TODO: need to remove passing tpProxy
    libProxy = UnionFindLib::unionFindInit();
    CkCallback cb(CkIndex_Main::done(), thisProxy);
    libProxy[0].register_phase_one_cb(cb);
    tpProxy = CProxy_TreePiece::ckNew();
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
    CkExit();
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
  UnionFindLib *libPtr;
  unionFindVertex *libVertices;

  public:
  // function that must be always defined by application
  // return type -> std::pair<int, int>
  // this specific logic assumes equal distribution of vertices across all tps
  static std::pair<int64_t, int64_t> getLocationFromID(int64_t vid);

  TreePiece() {
    myID = CkMyPe();
    numMyVertices = num_vertices / num_treepieces;
    if ((myID + 1) <= (num_vertices % num_treepieces)) {
      numMyVertices++;
    }

    libPtr = libProxy.ckLocalBranch();
    // only 1 chare in PE (group)
    libPtr->allocate_libVertices(numMyVertices, 1);

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

    numMyEdges = num_edges / num_treepieces;
    if (myID == (num_treepieces - 1)) {
      // last chare should get all remaining edges if not equal division
      numMyEdges += num_edges % num_treepieces;
    }
    // TODO
    populateMyEdges(&library_requests, numMyEdges, (num_edges/num_treepieces), thisIndex, num_vertices);
    libPtr->registerGetLocationFromID(getLocationFromID);
    contribute(CkCallback(CkReductionTarget(Main, startWork), mainProxy));
  }

  TreePiece(CkMigrateMessage *msg) { }

  void doWork() {
    // vertices and edges populated, now fire union requests
    for (int i = 0; i < library_requests.size(); i++) {
      std::pair<int64_t, int64_t> req = library_requests[i];
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

std::pair<int64_t, int64_t>
TreePiece::getLocationFromID(int64_t vid) {
  int64_t chareIdx = vid % num_treepieces;
  int64_t arrIdx = vid / num_treepieces;

  return std::make_pair(chareIdx, arrIdx);
}

#include "graph.def.h"
