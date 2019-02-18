#include <assert.h>
#include <iostream>
#include "unionFindLib.h"
#include "graph.decl.h"
#include "vtype.h"
#include "graph500-gen/make_graph.h"

/*readonly*/ CProxy_UnionFindLib libProxy;
/*readonly*/ CProxy_Main mainProxy;
/*readonly*/ int64_t num_vertices;
/*readonly*/ int64_t num_edges;
/*readonly*/ int64_t num_treepieces;

class Main : public CBase_Main {
  CProxy_TreePiece tpProxy;
  double startTime;

  int desired_num_edges;
  uint8_t scale;
  uint8_t edgeFactor;
  std::vector<std::pair<int64_t, int64_t>> edgeList;
  std::vector<std::vector<std::pair<int64_t, int64_t>>> splitEdgeList;

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

    // TODO: need to remove passing tpProxy
    libProxy = UnionFindLib::unionFindInit();
    CkCallback cb(CkIndex_Main::done(), thisProxy);
    libProxy[0].register_phase_one_cb(cb);
    tpProxy = CProxy_TreePiece::ckNew();
    // find first vertex ID on last chare
    // create a callback for library to inform application after
    // completing inverted tree construction

    // Start timing, include graph generation time
    startTime = CkWallTimer();

    // Generate kronecker graph (parallelized with MPI)
    int64_t seeds[2] = {1, 2};
    const double initiator[4] = {.57, .19, .19, .05};
    int64_t created_num_edges;
    int64_t* edges;

    CkPrintf("Generating kronecker graph...\n");
    make_graph(scale, desired_num_edges, seeds[0], seeds[1], initiator, &num_edges, &edges);

    for (int i = 0; i < num_edges; i++) {
      int64_t src = edges[2*i];
      int64_t dest = edges[2*i+1];

      if ((src >= 0) && (dest >= 0)) {
        // Valid edge
        edgeList.emplace_back(src, dest);
      }
    }

    if (num_edges != edgeList.size()) {
      num_edges = edgeList.size();
      CkPrintf("Updating num_edges to %ld after graph generation\n", num_edges);
    }

    // Free temporary memory used in graph generation
    free(edges);

    // Split edge list for distribution to treepieces
    std::size_t const num_edges_tp = edgeList.size() / num_treepieces;
    for (int i = 0; i < num_treepieces-1; i++) {
      splitEdgeList.emplace_back(edgeList.begin() + num_edges_tp * i,
                                 edgeList.begin() + num_edges_tp * (i+1));
    }
    // Remaining edges go into last treepiece
    splitEdgeList.emplace_back(edgeList.begin() + num_edges_tp * (num_treepieces-1), edgeList.end());

    CkPrintf("Edge lists ready for distribution\n");
  }

  void startWork() {
    CkPrintf("[Main] Library array with %d chares created and proxy obtained\n", num_treepieces);

    // Send edges to treepieces and start union-find algorithm
    // TODO Use scatter instead of point-to-point messages
    for (int i = 0; i < num_treepieces; i++) {
      tpProxy[i].doWork(splitEdgeList[i]);
    }
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
  int64_t numMyVertices;
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

    int64_t dummy = 0;
    int64_t offset = myID * numMyVertices; // The last PE might have different number of vertices
    int64_t startID = myID;

    libPtr = libProxy.ckLocalBranch();
    libPtr->allocate_libVertices(numMyVertices, 1);
    libPtr->initialize_vertices(numMyVertices, libVertices, dummy /* offset */,
                                999999999 /* batchSize - need to turn off */);

    for (int64_t i = 0; i < numMyVertices; i++) {
      libVertices[i].vertexID = libVertices[i].parent = startID;
      startID += num_treepieces;
      std::pair<int64_t, int64_t> w_id = getLocationFromID(libVertices[i].vertexID);
      // assert (w_id.first == myID);
      // CkPrintf("vertexID: %ld i: %ld w_id.second: %ld\n", libVertices[i].vertexID, i, w_id.second);
      assert (w_id.second == i);
    }
    libPtr->registerGetLocationFromID(getLocationFromID);

    contribute(CkCallback(CkReductionTarget(Main, startWork), mainProxy));
  }

  TreePiece(CkMigrateMessage *msg) { }

  void doWork(std::vector<std::pair<int64_t, int64_t>> edgeList) {
    // Fire union requests with the received edges
    for (auto edge : edgeList) {
      libPtr->union_request(edge.first, edge.second);
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
