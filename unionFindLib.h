#ifndef UNION_FIND_LIB
#define UNION_FIND_LIB

#include "unionFindLib.decl.h"
#include <NDMeshStreamer.h>

struct unionFindVertex {
    int64_t vertexID;
    int64_t parent;
    int64_t componentNumber = -1;
    std::vector<int64_t> need_boss_requests; //request queue for processing need_boss requests
    int64_t findOrAnchorCount = 0;

    void pup(PUP::er &p) {
        p|vertexID;
        p|parent;
        p|componentNumber;
        p|need_boss_requests;
    }
};


/* global variables */
/*readonly*/ extern CkGroupID libGroupID;

// class definition for library chares
class UnionFindLib : public CBase_UnionFindLib {
    std::vector<unionFindVertex> myVertices;
    int64_t numMyVertices;
    int64_t pathCompressionThreshold = 5;
    int componentPruneThreshold;
    std::pair<int64_t, int64_t> (*getLocationFromID)(int64_t vid);
    int64_t myLocalNumBosses;
    int64_t totalNumBosses;
    CkCallback postComponentLabelingCb;
    
    // Batch
    int64_t numCharesinPe;
    int64_t batchSize;
    int64_t batchNo;
    int64_t reqsProcessed;
    int64_t totalReqsPerBatch;
    int64_t thresholdReqs;
    int64_t totalReqsProcessed;
    CkCallback batchCb;

    // path compression
    std::vector<int64_t> verticesToCompress;

    // component labeling
    uint64_t reqs_sent;
    uint64_t reqs_recv;
    std::map<int64_t, std::vector<int64_t> > need_label_reqs;
  public:
    void need_label(int64_t req_vertex, int64_t parent_arrID);
    void recv_label(int64_t recv_vertex_arrID, int64_t labelID);

    public:
    UnionFindLib() {
      reqs_sent = 0;
      reqs_recv = 0;
    }
    UnionFindLib(CkMigrateMessage *m) { }
    static CProxy_UnionFindLib unionFindInit(CkArrayID clientArray, int64_t n);
    void register_phase_one_cb(CkCallback cb);
    // void initialize_vertices(unionFindVertex *appVertices, int numVertices);
    void allocate_libVertices(int64_t numVertices, int64_t nPe);
    void initialize_vertices(int64_t numVertices, unionFindVertex* &appVertices, int64_t &offset, int64_t bs);

    void union_request(int64_t v, int64_t w);
    void anchor(int64_t w_arrIdx, int64_t v, int64_t path_base_arrIdx);
    void local_path_compression(unionFindVertex *src, int64_t compressedParent);
    bool check_same_chares(int64_t v1, int64_t v2);
    void registerGetLocationFromID(std::pair<int64_t, int64_t> (*gloc)(int64_t v));
    void printVertices();

    // functions and data structures for finding connected components
    void reqs_processed();
    void recv_reqs_processed();
    void register_batch_cb(CkCallback cb);


    public:
    void find_components(CkCallback cb);
    void insertDataFindBoss(const findBossData & data);
    void start_component_labeling();
    void insertDataNeedBoss(const uint64_t & data);
    void insertDataAnchor(const anchorData & data);
    void need_boss(int64_t arrIdx, int64_t fromID);
    void set_component(int64_t arrIdx, int64_t compNum);
    void prune_components(int64_t threshold, CkCallback appReturnCb);
    void perform_pruning();
    int get_total_num_bosses() {
        return totalNumBosses;
    }
    //void merge_count_results(CkReductionMsg *msg);
    //void merge_count_results(int* totalCounts, int numElems);
#ifdef PROFILING
    void profiling_count_max(int64_t maxCount);
#endif
};

// library group chare class declarations
class UnionFindLibGroup : public CBase_UnionFindLibGroup {
    bool map_built;
    int64_t* component_count_array;
    int64_t thisPeMessages; //for profiling
    public:
    UnionFindLibGroup() {
        map_built = false;
        thisPeMessages = 0;
    }
    void build_component_count_array(int64_t* totalCounts, int64_t numComponents);
    int64_t get_component_count(int64_t component_id);
    void increase_message_count();
    void contribute_count();
    void done_profiling(int64_t);
};





#define CK_TEMPLATES_ONLY
#include "unionFindLib.def.h"
#undef CK_TEMPLATES_ONLY

#endif
