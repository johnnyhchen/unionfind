#include <assert.h>
#include "prefixBalance.h"
#include "unionFindLib.h"

/*readonly*/ CProxy_UnionFindLib _UfLibProxy;
/*readonly*/ CProxy_Prefix prefixLibArray;
/*readonly*/ CkGroupID libGroupID;
CkReduction::reducerType mergeCountMapsReductionType;

// custom reduction for merging local count maps
CkReductionMsg* merge_count_maps(int nMsgs, CkReductionMsg **msgs) {
    std::unordered_map<long int,int> merged_temp_map;
    for (int i = 0; i < nMsgs; i++) {
        // any sanity check for map size?
        // extract this message's local map
        componentCountMap *curr_map = (componentCountMap*)msgs[i]->getData();
        int numComps = msgs[i]->getSize();
        numComps = numComps / sizeof(componentCountMap);

        // convert custom map to STL map for easier lookup
        for (int j = 0; j < numComps; j++) {
            merged_temp_map[curr_map[j].compNum] += curr_map[j].count;
        }
    } // all messages processed

    // convert the STL back to custom map for messaging
    componentCountMap *merged_map = new componentCountMap[merged_temp_map.size()];
    std::unordered_map<long int,int>::iterator iter = merged_temp_map.begin();
    for (int i = 0; i < merged_temp_map.size(); i++) {
        componentCountMap entry;
        entry.compNum = iter->first;
        entry.count = iter->second;
        merged_map[i] = entry;
        iter++;
    }

    int retSize = sizeof(componentCountMap) * merged_temp_map.size();
    return CkReductionMsg::buildNew(retSize, merged_map);
}

// initnode function to register reduction
static void register_merge_count_maps_reduction() {
    mergeCountMapsReductionType = CkReduction::addReducer(merge_count_maps);
}

// class function implementations

void UnionFindLib::
registerGetLocationFromID(std::pair<int64_t, int64_t> (*gloc)(int64_t vid)) {
    getLocationFromID = gloc;
}

void UnionFindLib::
register_phase_one_cb(CkCallback cb) {
    if (CkMyPe() != 0)
        CkAbort("[UnionFindLib] Phase 1 callback must be registered on first PE only!");

    CkStartQD(cb);
}

// Called only by the one chare in the PE
void UnionFindLib::
allocate_libVertices(int64_t numVertices, int64_t nPe)
{
  // assert (myVertices.size() == 0);
  numCharesinPe = nPe;
  if (CkMyPe() == 0) {
    CkPrintf("Trying to allocate myVertices in library size: %lf GB myPE: %d elements: %ld numVertices: %ld numCharesinPe: %ld\n", (double)(sizeof(unionFindVertex) * numVertices * numCharesinPe) / (1024 * 1024 * 1024), CkMyPe(), (numVertices * numCharesinPe), numVertices, numCharesinPe);
  }   
  try {
    // myVertices = new unionFindVertex[numVertices * numCharesinPe];
    myVertices.resize((numVertices * numCharesinPe));
  }
  catch (const std::bad_alloc& ba) {
    ckout << "mem alloc error in library: " << ba.what() << endl;
    CkExit();
  }

  // CkPrintf("PE: %d, calling allocate for: %ld\n", CkMyPe(), (numVertices * numCharesinPe));
}

// batchSize should be -1 if all the union_requests are to be handled at once
void UnionFindLib::
initialize_vertices(int64_t numVertices, unionFindVertex* &appVertices, int64_t &offset, int64_t bs) {
    batchSize = bs;
    totalReqsPerBatch = batchSize * numCharesinPe;
    thresholdReqs = totalReqsPerBatch / CkNumPes();
    batchNo = 1;
    reqsProcessed = 0;
    totalReqsProcessed = 0;
    // local vertices corresponding to one treepiece in application
    numMyVertices = numVertices;
    if (offset == -1) {
      offset = myVertices.size();
      myVertices.resize(numVertices);
      appVertices = &myVertices[myVertices.size() - numMyVertices];
    }
    else {
      // The application provides the offset; application's main chare has already called allocate_libVertices()
      /*
      if ((offset + numMyVertices) > myVertices.size()) {
        CkPrintf("offset: %ld numMyVertices: %ld myVertices.size(): %d\n", offset, numMyVertices, myVertices.size());
      }
      CkPrintf("offset: %ld numMyVertices: %ld myVertices.size(): %d\n", offset, numMyVertices, myVertices.size());
      */
      assert ((offset + numMyVertices) <= myVertices.size());
      appVertices = &myVertices[offset];
    }
}

void UnionFindLib::
register_batch_cb(CkCallback cb) {
  if (CkMyPe() != 0) {
    CkAbort("[UnionFindLib] Batch callback must be registered on first PE only!");
  }
  batchCb = cb;
}

void UnionFindLib::
reqs_processed() {
  reqsProcessed++;
  if (reqsProcessed >= thresholdReqs) {
    // Communicate to node 0
    thisProxy[0].recv_reqs_processed();
    reqsProcessed = 0;
  }
}

// Only on node 0
void UnionFindLib::
recv_reqs_processed() {
  totalReqsProcessed += thresholdReqs;
  double th = 0.5 * totalReqsPerBatch * batchNo; // How many requests should have been processed till now
  if (totalReqsProcessed > th) {
    // CkPrintf("Batch: %ld done totalReqsProcessed: %ld\n", batchNo, totalReqsProcessed);
    batchNo++;
    // Broadcast a message to all application PEs using a callback
    batchCb.send();
  }
}

void UnionFindLib::
union_request(int64_t v, int64_t w) {
    /*
    if (v < 0 || w < 0)
      CkPrintf("v: %ld w: %ld\n", v, w);
    */

    std::pair<int64_t, int64_t> w_loc = getLocationFromID(w);
    
    // std::pair<int, int> v_loc = getLocationFromID(v);
    // CkPrintf("w_id: %ld v_id: %ld w: %d %d v: %d %d\n", w, v, w_loc.first, w_loc.second, v_loc.first, v_loc.second);
    // message w to anchor to v
    anchorData d;
    d.arrIdx = w_loc.second;
    d.v = v;
    // assert(w_loc.first >= 0);
    // assert(w_loc.first < CkNumPes());
    // assert(w_loc.second >= 0 && w_loc.second < 64);
    
    /*
    if (w_loc.first != 0)
      CkPrintf("sending to PE: %d\n", w_loc.first);
    */
    thisProxy[w_loc.first].insertDataAnchor(d);
}

void UnionFindLib::
anchor(int64_t w_arrIdx, int64_t v, int64_t path_base_arrIdx) {
    unionFindVertex *w = &myVertices[w_arrIdx];

    // CkPrintf("anchor() w_arrIdx: %d w->vertexID: %ld to vid: %ld\n", w_arrIdx, w->vertexID, v);
    w->findOrAnchorCount++;

    if (w->parent == v) {
      // call local_path_compression with v as parent
      if (path_base_arrIdx != -1) {
        unionFindVertex *path_base = &myVertices[path_base_arrIdx];
        local_path_compression(path_base, v);
      }
      reqs_processed();
      return;
    }

    if (w->vertexID < v) {
        // incorrect order, swap the vertices
        std::pair<int64_t, int64_t> v_loc = getLocationFromID(v);
        // if (v_loc.first == thisIndex) {
        if (v_loc.first == CkMyPe()) {
            // vertex available locally, avoid extra message
            if (path_base_arrIdx != -1) {
              // Have to change the direction; so compress path for w
              unionFindVertex *path_base = &myVertices[path_base_arrIdx];
              // FIXME: what happens if w is not in this chare?
              local_path_compression(path_base, w->vertexID);
            }
            // start a new base since I am changing direction; can't carry the old one
            path_base_arrIdx = v_loc.second; 
            // anchor(v_loc.second, w->parent, path_base_arrIdx);
            anchor(v_loc.second, w->parent, -1);
            return;
        }
        /*
        else {
          UnionFindLib *lc = thisProxy[v_loc.first].ckLocal();
          if (lc != nullptr) {
            // Moving away from this chare; see if local_path_compression should be done
            // FIXME: still should be able to do local_compression within node, but across chares
            if (path_base_arrIdx != -1) {
              unionFindVertex *path_base = &myVertices[path_base_arrIdx];
              local_path_compression(path_base, w->vertexID);
            }
            lc->anchor(v_loc.second, w->parent, -1);
            return;
          }
        }
        */
        anchorData d;
        d.arrIdx = v_loc.second;
        d.v = w->parent;
        // assert(v_loc.first >= 0);
        // assert(v_loc.first < CkNumPes());

        /*
        if (v_loc.first != 0)
          CkPrintf("sending to PE: %d\n", v_loc.first);
        */
        // assert(v_loc.second >= 0 && v_loc.second < 64);
        thisProxy[v_loc.first].insertDataAnchor(d);
    }
    else if (w->parent == w->vertexID) {
      // I have reached the root; check if I can call local_path_compression
      if (path_base_arrIdx != -1) {
        unionFindVertex *path_base = &myVertices[path_base_arrIdx];
        // Make all nodes point to this parent v
        local_path_compression(path_base, v);
      }
      w->parent = v;
      reqs_processed();
    }
    else {
        // call anchor for w's parent
        std::pair<int64_t, int64_t> w_parent_loc = getLocationFromID(w->parent);
        if (w_parent_loc.first == CkMyPe()) {
            if (path_base_arrIdx == -1) {
              // Start from w; a wasted call if there is only one node and its child in the PE
              std::pair<int64_t, int64_t> w_loc = getLocationFromID(w->vertexID);
              path_base_arrIdx = w_loc.second; 
            }
            else {
              std::pair<int64_t, int64_t> w_loc = getLocationFromID(w->vertexID);
              // assert (path_base_arrIdx != w_loc.second);
            }
            // anchor(w_parent_loc.second, v, -1);
            anchor(w_parent_loc.second, v, path_base_arrIdx);
            return;
        }
        else {
          // Moving away from this node; see if local_path_compression should be done
          if (path_base_arrIdx != -1) {
            unionFindVertex *path_base = &myVertices[path_base_arrIdx];
            // Make all nodes point to this parent w
            // assert (path_base->vertexID != w->vertexID);
            local_path_compression(path_base, w->vertexID);
          }
          /*
          UnionFindLib *lc = thisProxy[w_parent_loc.first].ckLocal();
          if (lc != nullptr) {
            // FIXME: still should be able to do local_compression within node, but across chares
            lc->anchor(w_parent_loc.second, v, -1);
            return;
          }
          */
        }
        anchorData d;
        d.arrIdx = w_parent_loc.second;
        d.v = v;
        // assert(w_parent_loc.first >= 0);
        // assert(w_parent_loc.first < CkNumPes());

         // assert(w_parent_loc.second >= 0 && w_parent_loc.second < 64);
        /*
        if (w_parent_loc.first != 0)
          CkPrintf("sending to PE: %d\n", w_parent_loc.first);
        */
        thisProxy[w_parent_loc.first].insertDataAnchor(d);
    }
}

// perform local path compression
void UnionFindLib::
local_path_compression(unionFindVertex *src, int64_t compressedParent) {
    unionFindVertex* tmp;
    // An infinite loop if this function is called on itself (a node which does not have itself as its parent)
    while (src->parent != compressedParent) {
        // CkPrintf("Stuck here\n");
        tmp = &myVertices[getLocationFromID(src->parent).second];
        src->parent = compressedParent;
        src =tmp;
    }
}

// check if two vertices are on same chare
bool UnionFindLib::
check_same_chares(int64_t v1, int64_t v2) {
    std::pair<int64_t, int64_t> v1_loc = getLocationFromID(v1);
    std::pair<int64_t, int64_t> v2_loc = getLocationFromID(v2);
    if (v1_loc.first == v2_loc.first)
        return true;
    return false;
}

/** Functions for finding connected components **/

void UnionFindLib::
find_components(CkCallback cb) {
    postComponentLabelingCb = cb;
    // count local numBosses
    myLocalNumBosses = 0;
    for (int64_t i = 0; i < numMyVertices; i++) {
        if (myVertices[i].parent == myVertices[i].vertexID) {
            myLocalNumBosses += 1;
        }
    }

    // send local count to prefix library
    CkCallback doneCb(CkReductionTarget(UnionFindLib, boss_count_prefix_done), thisProxy);
    // Prefix* myPrefixElem = prefixLibArray[thisIndex].ckLocal();
    // myPrefixElem->startPrefixCalculation(myLocalNumBosses, doneCb);
    prefixLibArray[thisIndex].startPrefixCalculation(myLocalNumBosses, doneCb);
    //CkPrintf("[%d] Local num bosses: %d\n", thisIndex, myLocalNumBosses);
}

// Recveive total boss count from prefix library and start labelling phase
void UnionFindLib::
boss_count_prefix_done(int64_t totalCount) {
    totalNumBosses = totalCount;
    // access value from prefix lib elem to find starting index
    Prefix* myPrefixElem = prefixLibArray[thisIndex].ckLocal();
    int64_t v = myPrefixElem->getValue();
    int64_t myStartIndex = v - myLocalNumBosses;
    //CkPrintf("[%d] My start index: %d\n", thisIndex, myStartIndex);

    // start labeling my local bosses from myStartIndex
    // ensures sequential numbering of components
    if (myLocalNumBosses != 0) {
        for (int64_t i = 0; i < numMyVertices; i++) {
            if (myVertices[i].parent == myVertices[i].vertexID) {
                myVertices[i].componentNumber = myStartIndex;
                myStartIndex++;
            }
        }
    }

    CkAssert(myStartIndex == v);

    // start the labeling phase for all vertices
    start_component_labeling();
}

void UnionFindLib::
start_component_labeling() {
    for (int64_t i = 0; i < numMyVertices; i++) {
        unionFindVertex *v = &myVertices[i];
        if (v->parent == v->vertexID) {
            // one of the bosses/root found
            CkAssert(v->componentNumber != -1); // phase 2a assigned serial numbers
            set_component(i, v->componentNumber);
        }

        if (v->componentNumber == -1) {
            // an internal node or leaf node, request parent for boss
            std::pair<int64_t, int64_t> parent_loc = getLocationFromID(v->parent);
            //this->thisProxy[parent_loc.first].need_boss(parent_loc.second, v->vertexID);
            uint64_t data = ((uint64_t) parent_loc.second) << 32 | ((uint64_t) v->vertexID);
            this->thisProxy[parent_loc.first].insertDataNeedBoss(data);
        }
    }

    if (this->thisIndex == 0) {
        // return back to application after completing all messaging related to
        // connected components algorithm
        CkStartQD(postComponentLabelingCb);
    }
}

void UnionFindLib::
insertDataFindBoss(const findBossData & data) {
#ifndef ANCHOR_ALGO
    if (data.isFBOne == 1) {
        this->find_boss1(data.arrIdx, data.partnerOrBossID, data.senderID);
    }
    else {
        this->find_boss2(data.arrIdx, data.partnerOrBossID, data.senderID);
    }
#endif
}

void UnionFindLib::
insertDataNeedBoss(const uint64_t & data) {
    int64_t arrIdx = (int64_t)(data >> 32);
    int64_t fromID = (int64_t)(data & 0xffffffff);
    this->need_boss(arrIdx, fromID);
}

void UnionFindLib::
insertDataAnchor(const anchorData & data) {
    anchor(data.arrIdx, data.v, -1);
}

void UnionFindLib::
printVertices() {
  for (int64_t i = 0; i < myVertices.size(); i++)
    CkPrintf("i: %d vertexID: %ld\n", i, myVertices[i].vertexID);
}

void UnionFindLib::
need_boss(int64_t arrIdx, int64_t fromID) {
    // one of children of this node needs boss, handle by either replying immediately
    // or queueing the request

    if (myVertices[arrIdx].componentNumber != -1) {
        // component already set, reply back
        std::pair<int64_t, int64_t> requestor_loc = getLocationFromID(fromID);
        if (requestor_loc.first == thisIndex)
            set_component(requestor_loc.second, myVertices[arrIdx].componentNumber);
        else
            this->thisProxy[requestor_loc.first].set_component(requestor_loc.second, myVertices[arrIdx].componentNumber);
    }
    else {
        // boss still not found, queue the request
        myVertices[arrIdx].need_boss_requests.push_back(fromID);
    }
}

void UnionFindLib::
set_component(int64_t arrIdx, int64_t compNum) {
    myVertices[arrIdx].componentNumber = compNum;

    // since component number is set, respond to your requestors
    std::vector<int64_t>::iterator req_iter = myVertices[arrIdx].need_boss_requests.begin();
    while (req_iter != myVertices[arrIdx].need_boss_requests.end()) {
        int64_t requestorID = *req_iter;
        std::pair<int64_t, int64_t> requestor_loc = getLocationFromID(requestorID);
        if (requestor_loc.first == thisIndex)
            set_component(requestor_loc.second, compNum);
        else
            this->thisProxy[requestor_loc.first].set_component(requestor_loc.second, compNum);
        // done with current requestor, delete from request queue
        req_iter = myVertices[arrIdx].need_boss_requests.erase(req_iter);
    }
}

void UnionFindLib::
prune_components(int64_t threshold, CkCallback appReturnCb) {
    componentPruneThreshold = threshold;

    //int *localCounts = new int[totalNumBosses]();
    std::vector<int64_t> localCounts(totalNumBosses, 0);
    //if (localCounts == NULL) {
    //    CkAbort("We are out of memory!");
    //}

    for (int64_t i = 0; i < numMyVertices; i++) {
        int64_t bossID = myVertices[i].componentNumber;
        CkAssert(bossID >= 0 && bossID < totalNumBosses);
        localCounts[bossID]++;
    }

    //CkPrintf("[TP %d] localCounts constructed\n", thisIndex);

    // bcast totalCounts to all group chares
    CProxy_UnionFindLibGroup libGroupProxy(libGroupID);
    CkCallback cb(CkReductionTarget(UnionFindLibGroup, build_component_count_array), libGroupProxy);
    //contribute(sizeof(int)*totalNumBosses, localCounts, CkReduction::sum_int, cb);
    contribute(localCounts, CkReduction::sum_long_long, cb);

    //delete[] localCounts;

    // start QD to return back to application
    if (thisIndex == 0) {
        CkStartQD(appReturnCb);
    }
}

// reductiontarget from group => all component count arrays are ready
void UnionFindLib::
perform_pruning() {

    CProxy_UnionFindLibGroup libGroup(libGroupID);

    for (int64_t i = 0; i < numMyVertices; i++) {
        int64_t myComponentCount = libGroup.ckLocalBranch()->get_component_count(myVertices[i].componentNumber);
        if (myComponentCount <= componentPruneThreshold) {
            myVertices[i].componentNumber = -1;
        }
#ifdef PROFILING
        //CkPrintf("Vertex ID : %d, count : %ld\n", myVertices[i].vertexID, myVertices[i].findOrAnchorCount);
#endif
    }

    if (thisIndex == 0) {
        CkPrintf("Number of components found: %d\n", totalNumBosses);
        int64_t numPrunedComponents = 0;
        for (int64_t i = 0; i < totalNumBosses; i++) {
            int64_t compCount = libGroup.ckLocalBranch()->get_component_count(i);
            if (compCount <= componentPruneThreshold) {
                numPrunedComponents++;
            }
        }
        //CkPrintf("Number of components after pruning: %d\n", totalNumBosses-numPrunedComponents);
    }

#ifdef PROFILING
    int64_t maxCount = -1;
    for (int64_t i = 0; i < numMyVertices; i++) {
        if (myVertices[i].findOrAnchorCount > maxCount)
            maxCount = myVertices[i].findOrAnchorCount;
    }
    CkCallback cb(CkReductionTarget(UnionFindLib, profiling_count_max), thisProxy[0]);
    contribute(sizeof(int64_t), &maxCount, CkReduction::max_long, cb);
#endif
}

#ifdef PROFILING
void UnionFindLib::
profiling_count_max(int64_t maxCount) {
    CkAssert(thisIndex == 0);
    CkPrintf("Max number of find/anchor messages per vertex: %ld\n", maxCount);
}
#endif

// library group chare class definitions
void UnionFindLibGroup::
build_component_count_array(int64_t *totalCounts, int64_t numElems) {
    //CkPrintf("[PE %d] Count array size: %d\n", thisIndex, numElems);
    component_count_array = new int64_t[numElems];
    memcpy(component_count_array, totalCounts, sizeof(int64_t)*numElems);
    contribute(CkCallback(CkReductionTarget(UnionFindLib, perform_pruning), _UfLibProxy));
}

int64_t UnionFindLibGroup::
get_component_count(int64_t component_id) {
    return component_count_array[component_id];
}

void UnionFindLibGroup::
increase_message_count() {
    thisPeMessages++;
}

void UnionFindLibGroup::
contribute_count() {
    CkCallback cb(CkReductionTarget(UnionFindLibGroup, done_profiling), thisProxy);
    contribute(sizeof(int64_t), &thisPeMessages, CkReduction::sum_long_long, cb);
}

void UnionFindLibGroup::
done_profiling(int64_t total_count) {
    if (CkMyPe() == 0) {
        CkPrintf("Phase 1 profiling done. Total number of messages is : %ld\n", total_count);
        CkExit();
    }
}

// library initialization function
CProxy_UnionFindLib UnionFindLib::
unionFindInit(CkArrayID clientArray, int64_t n) {
    /*  
    CkArrayOptions opts(n);
    opts.bindTo(clientArray);
    */
    _UfLibProxy = CProxy_UnionFindLib::ckNew();

    // create prefix library array here, prefix library is used in Phase 1B
    // Binding order: prefix -> unionFind -> app array
    
    CkArrayOptions prefix_opts(n);
    prefix_opts.bindTo(clientArray);
    prefixLibArray = CProxy_Prefix::ckNew(n, prefix_opts);
   

    libGroupID = CProxy_UnionFindLibGroup::ckNew();
    return _UfLibProxy;
}

#include "unionFindLib.def.h"
