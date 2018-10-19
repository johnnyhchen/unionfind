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

struct componentCountMap {
    int64_t compNum;
    int64_t count;

    void pup(PUP::er &p) {
        p|compNum;
        p|count;
    }
};


/* global variables */
/*readonly*/ extern CkGroupID libGroupID;
// declaration for custom reduction
extern CkReduction::reducerType mergeCountMapsReductionType;

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

    public:
    UnionFindLib() {}
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
    void boss_count_prefix_done(int64_t totalCount);
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

/// Some old functions for backup/reference ///

/*
void UnionFindLib::
start_boss_propagation() {
    // iterate over local bosses and send messages to requestors
    CkPrintf("Should never get executed!\n");
    std::vector<int>::iterator iter = local_boss_indices.begin();
    while (iter != local_boss_indices.end()) {
        int bossIdx = *iter;
        int64_t bossID = myVertices[bossIdx].vertexID;
        std::vector<int64_t>::iterator req_iter = myVertices[bossIdx].need_boss_requests.begin();
        while (req_iter != myVertices[bossIdx].need_boss_requests.end()) {
            int64_t requestorID = *req_iter;
            std::pair<int,int> requestor_loc = appPtr->getLocationFromID(requestorID);
            this->thisProxy[requestor_loc.first].set_component(requestor_loc.second, bossID);
            // done with requestor, delete from requests queue
            req_iter = myVertices[bossIdx].need_boss_requests.erase(req_iter);
        }
        // done with this local boss, delete from vector
        iter = local_boss_indices.erase(iter);
    }
}*/

/*
void UnionFindLib::
merge_count_results(CkReductionMsg *msg) {
    componentCountMap *final_map = (componentCountMap*)msg->getData();
    int numComps = msg->getSize();
    numComps = numComps/ sizeof(componentCountMap);

    if (this->thisIndex == 0) {
        CkPrintf("Number of components found: %d\n", numComps);
    }

    // convert custom map back to STL, for easier lookup
    std::map<int64_t,int> quick_final_map;
    for (int i = 0; i < numComps; i++) {
        if (this->thisIndex == 0) {
            //CkPrintf("Component %ld : Total vertices count = %d\n", final_map[i].compNum, final_map[i].count);
        }
        quick_final_map[final_map[i].compNum] = final_map[i].count;
    }

    for (int i = 0; i < numMyVertices; i++) {
        if (quick_final_map[myVertices[i].componentNumber] <= componentPruneThreshold) {
            // vertex belongs to a minor component, ignore by setting to -1
            myVertices[i].componentNumber = -1;
        }
    }

    delete msg;
}*/
