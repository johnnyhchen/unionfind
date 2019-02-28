#include <assert.h>
#include <unistd.h>
#include <iostream>
#include <string>
#include "unionFindLib.h"
#include "graph.decl.h"
#include "vtype.h"
#include "graph_io.h"
#include "make_graph.h"

/* readonly */ CProxy_UnionFindLib libProxy;
/* readonly */ CProxy_Main mainProxy;
/* readonly */ int64_t num_vertices;
/* readonly */ int64_t num_edges;
/* readonly */ int64_t num_treepieces;
/* readonly */ int64_t batch_size;

class Main : public CBase_Main {
  CProxy_TreePiece tpProxy;
  double startTime;

  int64_t desired_num_edges;
  uint8_t scale;
  uint8_t edge_factor;
  int64_t* edges;

  public:
  Main(CkArgMsg *m) {
    // Default parameters
    scale = 10; // 1M vertices
    edge_factor = 4; // 4M edges
    std::string input_file_name("");
    std::string output_file_name("");
    batch_size = 15000;

    // Command line parsing
    int c;
    while ((c = getopt(m->argc, m->argv, "s:e:b:i:o:h")) != -1) {
      switch (c) {
        case 's':
          scale = atoi(optarg);
          break;
        case 'e':
          edge_factor = atoi(optarg);
          break;
        case 'b':
          batch_size = atoi(optarg);
          break;
        case 'i':
          input_file_name = optarg;
          break;
        case 'o':
          output_file_name = optarg;
          break;
        case 'h':
          CkPrintf("Usage: ./graph -s [scale] -e [edge factor] -i [input file name]"
              " -o [output file name]");
          CkExit();
        case '?':
          if (optopt == 'c')
            CkError("Option -%c requires an argument.\n", optopt);
          else if (isprint(optopt))
            CkError("Unknown option '-%c'.\n", optopt);
          else
            CkError("Unknown option character '\\x%x'.\n", optopt);
          CkExit();
        default:
          CkAbort("Command line parsing error");
      }
    }

    num_vertices = 1UL << scale;
    desired_num_edges = edge_factor * num_vertices;
    CkPrintf("num_vertices: %ld, desired_num_edges: %ld\n", num_vertices, desired_num_edges);

    num_treepieces = CkNumPes();
    if (num_vertices < num_treepieces) {
      CkPrintf("Fewer vertices than treepieces\n");
      CkExit();
    }

    // Generate kronecker graph if input file is not given
    // (sequentially or with OpenMP, depends on linked library)
    if (input_file_name.length() == 0) {
      int64_t seeds[2] = {1, 2};
      const double initiator[4] = {.57, .19, .19, .05};

      double gen_start_time = CkWallTimer();
      CkPrintf("[Main] Input file not provided, generating kronecker graph...\n");
      make_graph(scale, desired_num_edges, seeds[0], seeds[1], initiator, &num_edges, &edges);
      CkPrintf("[Main] Graph generation time: %lf\n", CkWallTimer() - gen_start_time);

      if (output_file_name.length() > 0) {
        // TODO Write graph to file
        std::ofstream out_fs(output_file_name);
        CkPrintf("[Main] Writing graph to %s...\n", output_file_name.c_str());

        // Write metadata
        writeMetadata(out_fs, num_vertices, num_edges);

        // Write edges
        for (int64_t i = 0; i < num_edges; i++) {
          int64_t src = edges[2*i];
          int64_t dest = edges[2*i+1];

          if ((src >= 0) && (dest >= 0)) {
            out_fs << src << " " << dest << std::endl;
          }
        }

        // Free temporary memory used in graph generation
        free(edges);

        CkExit();
      }
    }
    else {
      // TODO Process input graph and store them in edges
    }

    // TODO: need to remove passing tpProxy
    libProxy = UnionFindLib::unionFindInit();
    CkCallback cb(CkIndex_Main::doneTreeGeneration(), thisProxy);
    libProxy[0].register_phase_one_cb(cb);
    tpProxy = CProxy_TreePiece::ckNew();
    CkCallback batch_cb(CkIndex_TreePiece::nextBatch(), tpProxy);
    libProxy[0].register_batch_cb(batch_cb);
    // find first vertex ID on last chare
    // create a callback for library to inform application after
    // completing inverted tree construction
  }

  void charesCreated() {
    CkPrintf("[Main] Library array with %d chares created and proxy obtained\n", num_treepieces);
    startTime = CkWallTimer();

    // Send edges to treepieces and start union-find algorithm
    // FIXME Use scatter instead of point-to-point messages
    const int64_t num_edges_tp = num_edges / num_treepieces;
    const int64_t num_edges_last_tp = num_edges_tp + (num_edges % num_treepieces);
    for (int i = 0; i < num_treepieces-1; i++) {
      tpProxy[i].receiveEdges(edges + num_edges_tp * i * 2, num_edges_tp * 2);
    }
    // Remaining edges go to last treepiece
    tpProxy[num_treepieces-1].receiveEdges(edges + num_edges_tp * (num_treepieces-1) * 2, num_edges_last_tp * 2);
  }

  void edgesReceived() {
    CkPrintf("[Main] All chares have received their edges\n");

    // Begin connected components detection
    tpProxy.sendRequests();
  }

  void doneTreeGeneration() {
    CkPrintf("[Main] Inverted trees constructed. Notify library to perform components detection\n");
    CkPrintf("[Main] Tree construction time: %lf\n", CkWallTimer() - startTime);
    // callback for library to inform application after completing
    // connected components detection
    CkCallback cb(CkIndex_Main::doneFindComponents(), thisProxy);
    //CkCallback cb(CkIndex_TreePiece::requestVertices(), tpProxy); //tmp, for debugging
    startTime = CkWallTimer();
    libProxy.find_components(cb);
  }

  void doneFindComponents() {
    CkPrintf("[Main] Components identified, prune unecessary ones now\n");
    CkPrintf("[Main] Components detection time: %lf\n", CkWallTimer() - startTime);
    // callback for library to report to after pruning
    CkExit();
    CkCallback cb(CkIndex_TreePiece::requestVertices(), tpProxy);
    libProxy.prune_components(1, cb);
  }

  void donePrinting() {
    CkPrintf("[Main] Final runtime: %lf\n", CkWallTimer() - startTime);
    CkExit();
  }
};

class TreePiece : public CBase_TreePiece {
  std::vector<proteinVertex> myVertices;
  int64_t numMyVertices;
  int64_t myID;
  UnionFindLib *libPtr;
  unionFindVertex *libVertices;
  int64_t* my_edges;
  int64_t num_my_edges;
  int64_t work_iter;
  int64_t num_requests;
  bool allow_batch;
  bool blocked_batch;

  public:
  // function that must be always defined by application
  // return type -> std::pair<int, int>
  // this specific logic assumes equal distribution of vertices across all tps
  static std::pair<int64_t, int64_t> getLocationFromID(int64_t vid);

  TreePiece() {
    myID = CkMyPe();
    numMyVertices = num_vertices / num_treepieces;
    work_iter = 0;
    blocked_batch = true;

    int64_t dummy = 0;
    int64_t offset = myID * numMyVertices; // The last PE might have different number of vertices
    int64_t startID = myID;

    libPtr = libProxy.ckLocalBranch();
    libPtr->allocate_libVertices(numMyVertices, 1);
    libPtr->initialize_vertices(numMyVertices, libVertices, dummy /* offset */,
                                batch_size);

    for (int64_t i = 0; i < numMyVertices; i++) {
      libVertices[i].vertexID = libVertices[i].parent = startID;
      startID += num_treepieces;
      std::pair<int64_t, int64_t> w_id = getLocationFromID(libVertices[i].vertexID);
      // assert (w_id.first == myID);
      // CkPrintf("vertexID: %ld i: %ld w_id.second: %ld\n", libVertices[i].vertexID, i, w_id.second);
      assert (w_id.second == i);
    }
    libPtr->registerGetLocationFromID(getLocationFromID);

    contribute(CkCallback(CkReductionTarget(Main, charesCreated), mainProxy));
  }

  TreePiece(CkMigrateMessage *msg) { }

  void receiveEdges(int64_t* e, int64_t nv) {
    my_edges = (int64_t*)malloc(nv * sizeof(int64_t));
    memcpy(my_edges, e, nv * sizeof(int64_t));
    num_my_edges = nv / 2;

    contribute(CkCallback(CkReductionTarget(Main, edgesReceived), mainProxy));
  }

  void sendRequests() {
    allow_batch = false;
    num_requests = 0;

    for (; work_iter < num_my_edges; work_iter++) {
      int64_t src = my_edges[2*work_iter];
      int64_t dest = my_edges[2*work_iter+1];
      if ((src >= 0 && src < num_vertices) && (dest >= 0 && dest < num_vertices)) {
        libPtr->union_request(src, dest);
        num_requests++;
      }

      if (num_requests >= batch_size) {
        if (allow_batch == false) {
          blocked_batch = true;
          break;
        }
      }
    }

    if (work_iter == num_my_edges) {
      blocked_batch = false;
    }
  }

  void nextBatch() {
    allow_batch = true;
    if (blocked_batch) {
      sendRequests();
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
