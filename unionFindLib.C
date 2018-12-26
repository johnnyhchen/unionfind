#include <assert.h>
#include "prefixBalance.h"
#include "unionFindLib.h"

/*readonly*/ CProxy_UnionFindLib _UfLibProxy;
/*readonly*/ CProxy_Prefix prefixLibArray;
/*readonly*/ CkGroupID libGroupID;

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
    CkPrintf("Trying to allocate myVertices in library size: %lf GB myPE: %d elements: %ld numVertices in each chare: %ld numCharesinPe: %ld\n", (double)(sizeof(unionFindVertex) * numVertices * numCharesinPe) / (1024 * 1024 * 1024), CkMyPe(), (numVertices * numCharesinPe), numVertices, numCharesinPe);
  }   
  try {
    // myVertices = new unionFindVertex[numVertices * numCharesinPe];
    myVertices.resize((numVertices * numCharesinPe));
  }
  catch (const std::bad_alloc& ba) {
    ckout << "mem alloc error in library: " << ba.what() << endl;
    CkExit();
  }
  totalVerticesinPE = myVertices.size();
  assert(totalVerticesinPE == (numVertices * numCharesinPe));
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
      assert (0); // TODO: check logic!
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
    std::pair<int64_t, int64_t> w_loc = getLocationFromID(w);
    if (w_loc.first == CkMyPe()) {
      verticesToCompress[0] = w_loc.second;
      anchor(w_loc.second, v);
    }
    else {
      anchorData d;
      d.arrIdx = w_loc.second;
      d.v = v;
      thisProxy[w_loc.first].insertDataAnchor(d);
    }
}

void UnionFindLib::
insertDataAnchor(const anchorData & data) {
    verticesToCompress[0] = data.arrIdx;
    anchor(data.arrIdx, data.v);
}

void UnionFindLib::
anchor(int64_t w_arrIdx, int64_t v) {
  unionFindVertex *w = &myVertices[w_arrIdx];

  // CkPrintf("anchor() w_arrIdx: %d w->vertexID: %ld to vid: %ld\n", w_arrIdx, w->vertexID, v);
  w->findOrAnchorCount++;

  if (w->parent == v) {
    // call local_path_compression with v as parent
    /*
    if (path_base_arrIdx != -1) {
      // unionFindVertex *path_base = &myVertices[path_base_arrIdx];
      verticesToCompress.push_back(path_base_arrIdx);
    }
    */
    std::pair<int64_t, int64_t> v_loc = getLocationFromID(v);
    if (v_loc.first == CkMyPe()) {
      local_path_compression(v);
    }
    else {
      local_path_compression(w->vertexID);
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
      /*
      if (path_base_arrIdx != -1) {
        // Have to change the direction; so compress path for w
        // unionFindVertex *path_base = &myVertices[path_base_arrIdx];
        // FIXME: what happens if w is not in this chare?
        // local_path_compression(path_base, w->vertexID);
      }
      */
      // start a new base since I am changing direction; can't carry the old one
      // path_base_arrIdx = v_loc.second; 
      // anchor(v_loc.second, w->parent, path_base_arrIdx);
      // check if this is the first zig-zag that is happening?
      if (verticesToCompress[1] == -1) {
        verticesToCompress[1] = v_loc.second;
      }
      anchor(v_loc.second, w->parent);
      return;
    }
    else {
      anchorData d;
      d.arrIdx = v_loc.second;
      d.v = w->parent;
      thisProxy[v_loc.first].insertDataAnchor(d);
      // moving away from this PE
      local_path_compression(w->vertexID);
    }
  }
  else if (w->parent == w->vertexID) {
    // I have reached the root; check if I can call local_path_compression
    /*
    if (path_base_arrIdx != -1) {
      // unionFindVertex *path_base = &myVertices[path_base_arrIdx];
      // Make all nodes point to this parent v
      // local_path_compression(path_base, v);
      verticesToCompress.push_back(path_base_arrIdx);
    }
    */
    std::pair<int64_t, int64_t> v_loc = getLocationFromID(v);
    if (v_loc.first == CkMyPe()) {
      local_path_compression(v);
    }
    else {
      local_path_compression(w->vertexID);
    }
    w->parent = v;
    reqs_processed();
  }
  else {
    // call anchor for w's parent
    std::pair<int64_t, int64_t> w_parent_loc = getLocationFromID(w->parent);
    if (w_parent_loc.first == CkMyPe()) {
      /*
      if (path_base_arrIdx == -1) {
        // Start from w; a wasted call if there is only one node and its child in the PE
        std::pair<int64_t, int64_t> w_loc = getLocationFromID(w->vertexID);
        path_base_arrIdx = w_loc.second;
        assert(path_base_arrIdx == w_arrIdx); 
      }
      */
      // anchor(w_parent_loc.second, v, -1);
      anchor(w_parent_loc.second, v);
      return;
    }
    else {
      // Moving away from this node; see if local_path_compression should be done
      /*
      if (path_base_arrIdx != -1) {
        // unionFindVertex *path_base = &myVertices[path_base_arrIdx];
        // Make all nodes point to this parent w
        // assert (path_base->vertexID != w->vertexID);
        // local_path_compression(path_base, w->vertexID);
        verticesToCompress.push_back(path_base_arrIdx);
      }
      */
      local_path_compression(w->vertexID);
      anchorData d;
      d.arrIdx = w_parent_loc.second;
      d.v = v;
      thisProxy[w_parent_loc.first].insertDataAnchor(d);
    }
  }
}

// perform local path compression
void UnionFindLib::
local_path_compression(int64_t compressedParent) {
  // for (std::vector<int64_t>::iterator it = verticesToCompress.begin() ; it != verticesToCompress.end(); ++it) {
  for (int i = 0; i < 2; i++) {
    int64_t arrIdx = verticesToCompress[i];
    if (arrIdx != -1) {
      // CkPrintf("Look i: %d\n", i);
      unionFindVertex *src = &myVertices[arrIdx];
      unionFindVertex* tmp;
      // An infinite loop if this function is called on itself (a node which does not have itself as its parent)
      // can be because it ends up going to src->parent and the path is not defined from there; should add src->vertexID != compressedParent
      // should and must have the above clause too; else a node might end up declaring itself as the root!
      // while (src->vertexID != compressedParent && /* TODO: not needed? */ src->parent != compressedParent) {
      while (src->vertexID > compressedParent) {
        // CkPrintf("Stuck here\n");
        std::pair<int64_t, int64_t> src_parent_loc = getLocationFromID(src->parent);
        src->parent = compressedParent;
        // assert(src->vertexID > compressedParent);
        if (src_parent_loc.first != CkMyPe()) {
          break;
        }
        tmp = &myVertices[src_parent_loc.second];
        src = tmp;
      }
    }
  }
  verticesToCompress[0] = verticesToCompress[1] = -1;
}


/** Functions for finding connected components **/
void UnionFindLib::
find_components(CkCallback cb) {
    postComponentLabelingCb = cb;

    /*
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
    */
    start_component_labeling();
}

void UnionFindLib::
start_component_labeling() {
  // Send requests only from those vertices whose parents are not in my PE
  myLocalNumBosses = 0;
  for (int64_t i = 0; i < totalVerticesinPE; i++) {
    unionFindVertex *v = &myVertices[i];
    if (v->parent == v->vertexID) {
      v->componentNumber = v->vertexID;
      myLocalNumBosses++;
      continue;
    }
    std::pair<int64_t, int64_t> parent_loc = getLocationFromID(v->parent);
    if (parent_loc.first != CkMyPe()) {
      thisProxy[parent_loc.first].need_label(v->vertexID, parent_loc.second);
      reqs_sent++;
      // Can there be a case where reqs_sent == reqs_recv; and still this PE is in this for-loop?
    }
  }
  // my PE is not sending any request
  if (reqs_sent == 0) {
    CkPrintf("PE: %d is not sending any requests!\n", CkMyPe());
    for (int64_t i = 0; i < totalVerticesinPE; i++) {
      unionFindVertex *v = &myVertices[i];
      if (v->componentNumber == -1) {
        // I don't have my label; does my parent have it?
        std::pair<int64_t, int64_t> parent_loc = getLocationFromID(v->parent);
        assert(parent_loc.first == CkMyPe());
        unionFindVertex *p = &myVertices[parent_loc.second];
        while (p->componentNumber == -1) {
          std::pair<int64_t, int64_t> gparent_loc = getLocationFromID(p->parent);
          assert(gparent_loc.first == CkMyPe());
          unionFindVertex *gp = &myVertices[gparent_loc.second];
          p = gp;
          // TODO: optimization possible here?
        }
        v->componentNumber = p->componentNumber;
      }
    }
    CkCallback cb(CkReductionTarget(UnionFindLib, total_components), thisProxy[0]);
    CkPrintf("PE: %d totalRoots: %lld\n", CkMyPe(), myLocalNumBosses);
    contribute(sizeof(int64_t), &myLocalNumBosses, CkReduction::sum_long_long, cb);
  }

  // if (this->thisIndex == 0) {
    // return back to application after completing all messaging related to
    // connected components algorithm
    // CkStartQD(postComponentLabelingCb);
  // }
}

void UnionFindLib::need_label(int64_t req_vertex, int64_t parent_arrID)
{
  // Traverse through the path and add it to the map of the vertex whose parent is not in this PE
  // TODO: opportunity to do local path compression here
  while (1) {
    unionFindVertex *p = &myVertices[parent_arrID];
    std::pair<int64_t, int64_t> gparent_loc = getLocationFromID(p->parent);
    if (p->parent == p->vertexID) {
      // found the component number; reply back to the requestor
      assert(p->componentNumber != -1);
      std::pair<int64_t, int64_t> req_loc = getLocationFromID(req_vertex);
      thisProxy[req_loc.first].recv_label(req_loc.second, p->componentNumber);
      break;
    }
    else if (gparent_loc.first != CkMyPe()) {
      // parent's parent not in this PE; add it to the map, and do nothing
      assert(p->componentNumber == -1); // not yet received the componentID; would have already sent a request
      need_label_reqs[p->vertexID].push_back(req_vertex);
      break;
    }
    else {
      // parent's parent is in this PE; set this as the parent for the next iteration
      parent_arrID = gparent_loc.second;
    }
  }
}

void UnionFindLib::recv_label(int64_t recv_vertex_arrID, int64_t labelID)
{
  assert(reqs_sent != 0);
  reqs_recv++;
  unionFindVertex *v = &myVertices[recv_vertex_arrID];
  assert(v->componentNumber == -1);
  v->componentNumber = labelID;
  // reply back to all those requests that were queued in this ID
  for (std::vector<int64_t>::iterator it = need_label_reqs[v->vertexID].begin() ; it != need_label_reqs[v->vertexID].end(); ++it) {
    std::pair<int64_t, int64_t> req_loc = getLocationFromID(*it);
    thisProxy[req_loc.first].recv_label(req_loc.second, labelID);
  }

  // all reqs received for my PE
  if (reqs_recv == reqs_sent) {
    for (int64_t i = 0; i < totalVerticesinPE; i++) {
      unionFindVertex *v = &myVertices[i];
      if (v->componentNumber == -1) {
        // I don't have my label; does my parent have it?
        std::pair<int64_t, int64_t> parent_loc = getLocationFromID(v->parent);
        assert(parent_loc.first == CkMyPe());
        unionFindVertex *p = &myVertices[parent_loc.second];
        while (p->componentNumber == -1) {
          std::pair<int64_t, int64_t> gparent_loc = getLocationFromID(p->parent);
          assert(gparent_loc.first == CkMyPe());
          unionFindVertex *gp = &myVertices[gparent_loc.second];
          p = gp;
          // TODO: optimization possible here?
        }
        v->componentNumber = p->componentNumber;
      }
    }
    CkCallback cb(CkReductionTarget(UnionFindLib, total_components), thisProxy[0]);
    CkPrintf("PE: %d totalRoots: %lld\n", CkMyPe(), myLocalNumBosses);
    contribute(sizeof(int64_t), &myLocalNumBosses, CkReduction::sum_long_long, cb);
  }
}

// Executed only on PE0
void UnionFindLib::total_components(int64_t nComponents)
{
  CkPrintf("Total components: %lld\n", nComponents);
  postComponentLabelingCb.send();
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

    for (int64_t i = 0; i < totalVerticesinPE; i++) {
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

    for (int64_t i = 0; i < totalVerticesinPE; i++) {
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
    for (int64_t i = 0; i < totalVerticesinPE; i++) {
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
