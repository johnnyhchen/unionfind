#include <assert.h>
#include "prefixBalance.h"
#include "unionFindLib.h"

/*readonly*/ CProxy_UnionFindLib _UfLibProxy;
/*readonly*/ CProxy_Prefix prefixLibArray;
/*readonly*/ CkGroupID libGroupID;
/*readonly*/ CProxy_UnionFindLibCache _UfLibProxyCache;
/*readonly*/ CkCallback initDoneCacheCb;
/*readonly*/ CkCallback libProxyDoneCb;
//CkCallback d;
//CkCallback UnionFindLib::libProxyDoneCb;


std::vector<unionFindVertex>& UnionFindLib::
ret_NodeMyVertices() 
{
  // CkPrintf("PE: %d myVerticesAddress from cache pointer: %p\n", CkMyPe(), myVertices.size(), _UfLibProxyCache.ckLocalBranch()->myVertices);
  return _UfLibProxyCache.ckLocalBranch()->myVertices;
}

// class function implementations
void UnionFindLib::
registerGetLocationFromID(std::pair<int64_t, int64_t> (*gloc)(int64_t vid)) {
    //CkPrintf("PE: %d registerGetLocationFromID\n", CkMyPe());
    getLocationFromID = gloc;
    //gloc(4);
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
  // _UfLibProxyCache.ckLocalBranch()->nPesinNode = _nPesinNode;
  //CkPrintf("PE: %d, calling allocate numVertices: %ld nPe: %ld\n", CkMyPe(), numVertices, nPe);
  numCharesinPe = nPe;
  //CkPrintf("PE: %d, calling allocate for: %ld\n", CkMyPe(), (numVertices * numCharesinPe));
  //CkPrintf("PE: %d, calling allocate for: %ld myVertices address from cache: %p\n", CkMyPe(), (numVertices * numCharesinPe), _UfLibProxyCache.ckLocalBranch()->myVertices);
  //CkPrintf("PE: %d, calling allocate for: %ld myVertices address: %p\n", CkMyPe(), (numVertices * numCharesinPe), &myVertices);
  //CkPrintf("PE: %d, calling allocate for: %ld myVertices.size(): %ld\n", CkMyPe(), (numVertices * numCharesinPe), myVertices.size());
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
  //CkPrintf("PE: %d, calling allocate for: %ld myVertices.size(): %ld\n", CkMyPe(), (numVertices * numCharesinPe), myVertices.size());
}

// Called only by rank-0 PE in the node
/*
void UnionFindLib::
allocate_libVertices_perNode(int64_t numVertices, int64_t nPe)
{
  // assert (myVertices.size() == 0);
  // numCharesinPe = nPe;
  numPEsinNode = nPe;
  if (CkMyPe() == 0) {
    CkPrintf("Trying to allocate myVertices in library size: %lf GB myPE: %d elements: %ld numVertices in each chare: %ld numPEsinNode: %ld\n", (double)(sizeof(unionFindVertex) * numVertices * numPEsinNode) / (1024 * 1024 * 1024), CkMyPe(), (numVertices * numPEsinNode), numVertices, numPEsinNode);
  }
  
  try {
    // myVertices = new unionFindVertex[numVertices * numCharesinPe];
    myVertices.resize((numVertices * numPEsinNode));
  }
  catch (const std::bad_alloc& ba) {
    ckout << "mem alloc error in library: " << ba.what() << endl;
    CkExit();
  }
  totalVerticesinNode = myVertices.size();
  assert(totalVerticesinNode == (numVertices * numPEsinNode));
  // CkPrintf("PE: %d, calling allocate for: %ld\n", CkMyPe(), (numVertices * numCharesinPe));
}
*/
// batchSize should be -1 if all the union_requests are to be handled at once
// offset should be according to the rank of the PE in the node
void UnionFindLib::
initialize_vertices(int64_t numVertices, unionFindVertex* &appVertices, int64_t &offset, int64_t bs) {
    //CkPrintf("PE: %d initialize_vertices()\n", CkMyPe());
    //CkPrintf("PE: %d initialize_vertices() myVertices.size() from cache proxy: %d\n", CkMyPe(), _UfLibProxyCache.ckLocalBranch()->myVertices.size());
    CkPrintf("PE: %d initialize_vertices() myVertices.size(): %d\n", CkMyPe(), myVertices.size());
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
      
      if ((offset + numMyVertices) > myVertices.size()) {
        CkPrintf("offset: %ld numMyVertices: %ld myVertices.size(): %d\n", offset, numMyVertices, myVertices.size());
      }
      /*
      CkPrintf("offset: %ld numMyVertices: %ld myVertices.size(): %d\n", offset, numMyVertices, myVertices.size());
      */
      
      assert ((offset + numMyVertices) <= myVertices.size());
      appVertices = &myVertices[offset];
    }
    _UfLibProxyCache.ckLocalBranch()->offsets[CkMyPe()] = offset;
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
  // FIXME: disabling batching temporarily
  return;
  totalReqsProcessed += thresholdReqs;
  double th = 0.5 * totalReqsPerBatch * batchNo; // How many requests should have been processed till now
  if (totalReqsProcessed > th) {
    CkPrintf("Batch: %ld done totalReqsProcessed: %ld th: %lf thresholdReqs: %ld\n", batchNo, totalReqsProcessed, th, thresholdReqs);
    batchNo++;
    // Broadcast a message to all application PEs using a callback
    batchCb.send();
  }
}

void UnionFindLib::
union_request(int64_t v, int64_t w) {
    totEdges++;
    /*
    if (v < 0 || w < 0)
      CkPrintf("v: %ld w: %ld\n", v, w);
    */

    //CkPrintf("PE: %d union v: %ld w: %ld\n", CkMyPe(), v, w);
    std::pair<int64_t, int64_t> w_loc = getLocationFromID(w);
    std::pair<int64_t, int64_t> v_loc = getLocationFromID(v);

    //CkPrintf("PE: %d union v: %ld w: %ld\n", CkMyPe(), v, w);
    // FIXME: bug - need to poll v & w if they are in same PE
    
    // Check if I can access the myVertices data: which means "both" w & v should be in my PE/node

    // if (w_loc.first == CkMyPe() && v_loc.first == CkMyPe()) {
    if (CmiNodeOf(w_loc.first) == CkMyNode() && CmiNodeOf(v_loc.first) == CkMyNode()) {
      if(myVertices[w_loc.second].componentNumber != -1 && (myVertices[w_loc.second].componentNumber == myVertices[v_loc.second].componentNumber)) {
        // the edge can be safely dropped
        droppedEdges++;
        return;
      }
    }

    // assert ((w_loc.first * (3072441 / 4) + w_loc.second) == w);
    
    // std::pair<int, int> v_loc = getLocationFromID(v);
    // CkPrintf("w_id: %ld v_id: %ld w: %d %d v: %d %d\n", w, v, w_loc.first, w_loc.second, v_loc.first, v_loc.second);
    // message w to anchor to v
    // assert(w_loc.first >= 0);
    // assert(w_loc.first < CkNumPes());
    // assert(w_loc.second >= 0 && w_loc.second < 64);
    
    /*
    if (w_loc.first != 0)
      CkPrintf("sending to PE: %d\n", w_loc.first);
    */
    // CkPrintf("Union request v: %ld w: %ld CkMyPe(): %d w_loc.first: %ld\n", v, w, CkMyPe(), w_loc.first);
    if (w_loc.first == CkMyPe()) {
      // insertDataAnchor(d);
      anchor(w_loc.second, v, -1);
    }
    else {
      anchorData d;
      d.arrIdx = w_loc.second;
      d.v = v;
      // CkPrintf("Sending message to PE: %d\n", w_loc.first);
      thisProxy[w_loc.first].insertDataAnchor(d);
    }
}

void UnionFindLib::
insertDataAnchor(const anchorData & data) {
    anchor(data.arrIdx, data.v, -1);
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
        // TODO: all children are made to point to v; if it is not in this PE; wasted need_root() calls
        local_path_compression(path_base, v);
      }
      reqs_processed();
      return;
    }

    if (w->vertexID < v) {
      if (path_base_arrIdx != -1) {
        // Have to change the direction; so compress path for w
        unionFindVertex *path_base = &myVertices[path_base_arrIdx];
        // TODO: why should local_path_compression() be done only if v is in this PE?
        local_path_compression(path_base, w->vertexID);
      }
      // incorrect order, swap the vertices
      std::pair<int64_t, int64_t> v_loc = getLocationFromID(v);
        // if (v_loc.first == thisIndex) {
        if (v_loc.first == CkMyPe()) {
            // vertex available locally, avoid extra message
            // start a new base since I am changing direction; can't carry the old one
            // path_base_arrIdx = v_loc.second; 
            // anchor(v_loc.second, w->parent, path_base_arrIdx);
            anchor(v_loc.second, w->parent, -1);
            return;
        }
        else {
          anchorData d;
          d.arrIdx = v_loc.second;
          d.v = w->parent;
          thisProxy[v_loc.first].insertDataAnchor(d);
        }
    }
    else if (w->parent == w->vertexID) {
      // I have reached the root; check if I can call local_path_compression
      if (path_base_arrIdx != -1) {
        unionFindVertex *path_base = &myVertices[path_base_arrIdx];
        // Make all nodes point to this parent v
        // TODO: all children are made to point to v; if it is not in this PE; wasted need_root() calls
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
              /*
              if (w_loc.second != w_arrIdx) {
                CkPrintf("myPE: %d vertexID: %ld w_loc.first: %ld w_loc.second: %ld w_arrIdx: %ld\n", CkMyPe(), w->vertexID, w_loc.first, w_loc.second, w_arrIdx);
              }
              */
              assert(w_loc.second == w_arrIdx); 
              path_base_arrIdx = w_loc.second;
              assert(path_base_arrIdx == w_arrIdx); 
            }
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
          anchorData d;
          d.arrIdx = w_parent_loc.second;
          d.v = v;
          thisProxy[w_parent_loc.first].insertDataAnchor(d);
        }
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
        assert(src->vertexID > compressedParent);
        src =tmp;
    }
}


void UnionFindLib::prepare_for_component_labeling(CkCallback cb)
{
  postPreCompLabCb = cb;
  if (true) {
    // reset the componentNumber for the next use
    for (int64_t i = 0; i < totalVerticesinPE; i++) {
      unionFindVertex *v = &myVertices[i];
      v->componentNumber = -1;
      assert(v->parent != -1);
      if (v->parent == v->vertexID) {
        v->componentNumber = v->vertexID;
      }
    }
    need_label_reqs.clear();
    reqs_sent = reqs_recv = 0;
    // resetData = false;
  }
  else {
    resetData = true;
  }
  contribute(CkCallback(CkReductionTarget(UnionFindLib, done_prepare_for_component_labeling), _UfLibProxy[0]));
}

void UnionFindLib::done_prepare_for_component_labeling()
{
  postPreCompLabCb.send();
}


void UnionFindLib::inter_start_component_labeling(CkCallback cb)
{
  postInterComponentLabelingCb = cb;
  // When this phase begins, only either inter_start_component_labeling() or inter_need_label() might be called
  /*
  if (resetData == true) {
    // reset the componentNumber for the next use
    for (int64_t i = 0; i < totalVerticesinPE; i++) {
      unionFindVertex *v = &myVertices[i];
      v->componentNumber = -1;
    }
    need_label_reqs.clear();
    reqs_sent = reqs_recv = 0;
    resetData = false;
  }
  else {
    // the inter_need_label() has already reset all the data, get it ready for the next phase
    resetData = true;
  }
  */

  myLocalNumBosses = 0;
  //int64_t off = _UfLibProxyCache.ckLocalBranch()->offsets[CkMyPe()];
  //CkPrintf("PE: %d off: %lld totalVerticesinPE: %lld\n", CkMyPe(), off, totalVerticesinPE);
  for (int64_t i = 0; i < totalVerticesinPE; i++) {
    unionFindVertex *v = &myVertices[i];
    // CkPrintf("PE: %d i: %lld v->vertexID: %lld v->parent: %lld\n", CkMyPe(), i, v->vertexID, v->parent);
    // CkPrintf("PE: %d v->vertexID: %lld\n", CkMyPe(), v->vertexID);
    if (v->parent == v->vertexID) {
      // v->componentNumber = v->vertexID;
      myLocalNumBosses++;
      continue;
    }
    std::pair<int64_t, int64_t> parent_loc = getLocationFromID(v->parent);
    // Send a request if this data is not in my node
    if (CmiNodeOf(parent_loc.first) != CkMyNode()) {
      //thisProxy[parent_loc.first].need_label(v->vertexID, parent_loc.second);
      needRootData d;
      d.req_vertex = v->vertexID;
      d.parent_arrID = parent_loc.second;
      thisProxy[parent_loc.first].inter_need_label(d);
      reqs_sent++;
      // Can there be a case where reqs_sent == reqs_recv; and still this PE is in this for-loop?
    }
  }
  // CkPrintf("PE: %d reqs_sent: %ld myLocalNumBosses: %lld\n", CkMyPe(), reqs_sent, myLocalNumBosses);
  // my PE is not sending any request
  if (reqs_sent == 0) {
    // CkPrintf("PE: %d reqs_sent = 0\n", CkMyPe());
    // CkPrintf("PE: %d is not sending any requests!\n", CkMyPe());
    for (int64_t i = 0; i < totalVerticesinPE; i++) {
      unionFindVertex *v = &myVertices[i];
      if (v->componentNumber == -1) {
        // I don't have my label; does my parent have it?
        std::pair<int64_t, int64_t> parent_loc = getLocationFromID(v->parent);
        assert(CmiNodeOf(parent_loc.first) == CkMyNode());
        unionFindVertex *p = &myVertices[parent_loc.second];
        while (p->componentNumber == -1) {
          std::pair<int64_t, int64_t> gparent_loc = getLocationFromID(p->parent);
          assert(CmiNodeOf(gparent_loc.first) == CmiNodeOf(CkMyPe()));
          unionFindVertex *gp = &myVertices[gparent_loc.second];
          p = gp;
          // TODO: optimization possible here?
        }
        v->componentNumber = p->componentNumber;
        v->parent = v->componentNumber;
        assert(v->parent != -1);
      }
    }

    CkCallback cb(CkReductionTarget(UnionFindLib, inter_total_components), thisProxy[0]);
    // CkPrintf("PE: %d totalRoots: %lld\n", CkMyPe(), myLocalNumBosses);
    int64_t vv[3];
    vv[0] = myLocalNumBosses;
    vv[1] = droppedEdges;
    vv[2] = totEdges;
    // contribute(sizeof(int64_t), &myLocalNumBosses, CkReduction::sum_long_long, cb);
    droppedEdges = 0;
    totEdges = 0;
    contribute(3*sizeof(int64_t), vv, CkReduction::sum_long_long, cb);
  }
}

void UnionFindLib::inter_need_label(needRootData data)
{
  // When this phase begins, only either inter_start_component_labeling() or inter_need_label() might be called
  /*
  if (resetData == true) {
    CkAssert(0); // Such a case cannot exist; inter_start_component_labeling() should always be called first; else use a different logic
    // reset the componentNumber for the next use
    for (int64_t i = 0; i < totalVerticesinPE; i++) {
      unionFindVertex *v = &myVertices[i];
      v->componentNumber = -1;
    }
    need_label_reqs.clear();
    reqs_sent = reqs_recv = 0;
    resetData = false;
  }
  else {
    // the inter_start_component_labeling() has already reset all the data, get it ready for the next phase
    // resetData = true;
  }
  */

  // Traverse through the path and add it to the map of the vertex whose parent is not in this PE
  // TODO: opportunity to do local path compression here
  // At the end of the phase the compression would anyway happen; just that intermediate requests need to again the traverse the path
  int64_t req_vertex = data.req_vertex;
  int64_t parent_arrID = data.parent_arrID;
  // CkPrintf("PE: %d req_vertex: %lld parent_arrID: %lld\n", CkMyPe(), req_vertex, parent_arrID);
  while (1) {
    unionFindVertex *p = &myVertices[parent_arrID];
    std::pair<int64_t, int64_t> gparent_loc = getLocationFromID(p->parent);
    // CkPrintf("PE: %d p->parent: %lld p->vertexID: %lld p->componentNumber: %lld gparent_loc.first: %lld gparent_loc.second: %lld\n", CkMyPe(), p->parent, p->vertexID, p->componentNumber, gparent_loc.first, gparent_loc.second);
    if (p->parent == p->vertexID || p->componentNumber != -1 /* I already have my componentNumber? TODO: check*/) {
      // found the component number; reply back to the requestor
      std::pair<int64_t, int64_t> req_loc = getLocationFromID(req_vertex);
      // CkPrintf("PE: %d req_loc.first: %lld req_loc.second: %lld\n", CkMyPe(), req_loc.first, req_loc.second);
      thisProxy[req_loc.first].inter_recv_label(req_loc.second, p->componentNumber);
      // if (p->componentNumber == -1) {
        // CkPrintf("Error here p->parent: %ld p->vertexID: %ld\n", p->parent, p->vertexID);
      // }
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
  //CkPrintf("PE: %d I reached here\n", CkMyPe());
}


void UnionFindLib::inter_recv_label(int64_t recv_vertex_arrID, int64_t labelID)
{
  assert(reqs_sent != 0);
  reqs_recv++; 
  // CkPrintf("PE: %d, reqs_recv: %lld\n", CkMyPe(), reqs_recv);
  unionFindVertex *v = &myVertices[recv_vertex_arrID];
  assert(v->componentNumber == -1);
  v->componentNumber = labelID;
  v->parent = labelID;
  assert(v->parent != -1);
  // reply back to all those requests that were queued in this ID
  for (std::vector<int64_t>::iterator it = need_label_reqs[v->vertexID].begin() ; it != need_label_reqs[v->vertexID].end(); ++it) {
    std::pair<int64_t, int64_t> req_loc = getLocationFromID(*it);
    thisProxy[req_loc.first].inter_recv_label(req_loc.second, labelID);
  }

  // all reqs received for my PE
  if (reqs_recv == reqs_sent) {
    CkPrintf("PE: %d reqs_sent == reqs_recv\n", CkMyPe());
    // int64_t off = _UfLibProxyCache.ckLocalBranch()->offsets[CkMyPe()];
    for (int64_t i = 0; i < totalVerticesinPE; i++) {
      unionFindVertex *v = &myVertices[i];
      if (v->componentNumber == -1) {
        // I don't have my label; does my parent have it?
        std::pair<int64_t, int64_t> parent_loc = getLocationFromID(v->parent);
        if (parent_loc.first != CkMyPe()) {
          CkPrintf("Error here in PE: %d parent_loc.first: %ld v->vertexID: %ld v->parent: %ld v->componentNumber: %ld\n", CkMyPe(), parent_loc.first, v->vertexID, v->parent, v->componentNumber);
        }
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
        v->parent = v->componentNumber;
      }
    }
    CkCallback cb(CkReductionTarget(UnionFindLib, inter_total_components), thisProxy[0]);
    // CkPrintf("PE: %d totalRoots: %lld\n", CkMyPe(), myLocalNumBosses);
    // contribute(sizeof(int64_t), &myLocalNumBosses, CkReduction::sum_long_long, cb);
    int64_t vv[3];
    vv[0] = myLocalNumBosses;
    vv[1] = droppedEdges;
    vv[2] = totEdges;
    droppedEdges = 0;
    totEdges = 0;
    contribute(3*sizeof(int64_t), vv, CkReduction::sum_long_long, cb);
  }
}

// Executed only on PE0
void UnionFindLib::inter_total_components(int n, int64_t* nComponents)
{
  CkPrintf("Total components in this phase: %lld droppedEdges: %lld totEdges: %lld percent_dropped: %lf\n", nComponents[0], nComponents[1], nComponents[2], (nComponents[1]/(double)nComponents[2]));
  postInterComponentLabelingCb.send();
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
  // int64_t off = _UfLibProxyCache.ckLocalBranch()->offsets[CkMyPe()];
  for (int64_t i = 0; i < totalVerticesinPE; i++) {
    unionFindVertex *v = &myVertices[i];
    if (v->parent == v->vertexID) {
      v->componentNumber = v->vertexID;
      myLocalNumBosses++;
      continue;
    }
    std::pair<int64_t, int64_t> parent_loc = getLocationFromID(v->parent);
    if (parent_loc.first != CkMyPe()) {
      //thisProxy[parent_loc.first].need_label(v->vertexID, parent_loc.second);
      needRootData d;
      d.req_vertex = v->vertexID;
      d.parent_arrID = parent_loc.second;
      thisProxy[parent_loc.first].need_label(d);
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
    // CkPrintf("PE: %d totalRoots: %lld\n", CkMyPe(), myLocalNumBosses);
    contribute(sizeof(int64_t), &myLocalNumBosses, CkReduction::sum_long_long, cb);
  }

  // if (this->thisIndex == 0) {
    // return back to application after completing all messaging related to
    // connected components algorithm
    // CkStartQD(postComponentLabelingCb);
  // }
}

//void UnionFindLib::need_label(int64_t req_vertex, int64_t parent_arrID)
void UnionFindLib::need_label(needRootData data)
{
  // Traverse through the path and add it to the map of the vertex whose parent is not in this PE
  // TODO: opportunity to do local path compression here
  int64_t req_vertex = data.req_vertex;
  int64_t parent_arrID = data.parent_arrID;
  while (1) {
    unionFindVertex *p = &myVertices[parent_arrID];
    std::pair<int64_t, int64_t> gparent_loc = getLocationFromID(p->parent);
    if (p->parent == p->vertexID || p->componentNumber != -1 /* I already have my componentNumber? TODO: check*/) {
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


// library cache initialization function
CProxy_UnionFindLibCache UnionFindLibCache::
UnionFindLibCacheInit(CkCallback cb) {
    initDoneCacheCb = cb;
    _UfLibProxyCache = CProxy_UnionFindLibCache::ckNew();
    CkPrintf("From library PE: %d libCacheProxy.id: %d\n", CkMyPe(), _UfLibProxyCache.ckGetGroupID().idx);
    return _UfLibProxyCache;
}


UnionFindLibCache::
UnionFindLibCache() {
  /*
  int nPesinNode = 0;
  for (int pe = 0; pe < CkNumPes(); pe++) {
    if (CmiNodeOf(pe) == CmiMyNode()) {
      nPesinNode++;
    }
  }
  offsets.resize(nPesinNode);
  CkPrintf("UnionFindLibCache() CmiMyNode(): %d nPesinNode: %d\n", CmiMyNode(), nPesinNode);
  */
  // Each nodegroup has the offset information for all PEs
  offsets.resize(CkNumPes(), 0);
  contribute(CkCallback(CkReductionTarget(UnionFindLibCache, initDoneCache), thisProxy[0]));
}

// library cache initialization function
void UnionFindLibCache::
initDoneCache() {
  CkPrintf("Node group creation done\n");
  initDoneCacheCb.send();
}

void UnionFindLibCache::
initOffsets(CkCallback _libcb) {
  // CkCallback cb(CkIndex_doneOffsets(), thisProxy[0]);
  callWorkCb = _libcb;
  CkCallback cb(CkReductionTarget(UnionFindLibCache, doneOffsets), thisProxy);
  contribute(offsets, CkReduction::sum_long_long, cb);
}

void UnionFindLibCache::
doneOffsets(std::vector<int64_t> result) {
  //doneOffsets(int64_t *result, int n) {
  offsets = result;
  contribute(CkCallback(CkReductionTarget(UnionFindLibCache, callWork), thisProxy[0]));
}

void UnionFindLibCache::
callWork() {
  callWorkCb.send();
}


// library initialization function for group
CProxy_UnionFindLib UnionFindLib::
unionFindInit(CkCallback _cb) {
    libProxyDoneCb = _cb;
    /*  
    CkArrayOptions opts(n);
    opts.bindTo(clientArray);
    */
    _UfLibProxy = CProxy_UnionFindLib::ckNew();

    // create prefix library array here, prefix library is used in Phase 1B
    // Binding order: prefix -> unionFind -> app array
    return _UfLibProxy;
}

UnionFindLib::
UnionFindLib() {
      droppedEdges = 0;
      totEdges = 0;
      reqs_sent = 0;
      reqs_recv = 0;
      resetData = false;
      CkPrintf("PE: %d myVertices.size(): %d myVerticesAddress: %p address from cache: %p cache proxy id: %d x_address: %p nodeId: %d\n", CkMyPe(), myVertices.size(), &myVertices, _UfLibProxyCache.ckLocalBranch()->myVertices, _UfLibProxyCache, &(_UfLibProxyCache.ckLocalBranch()->x), CmiNodeOf(CkMyPe()));
      // myVertices =  (_UfLibProxyCache.ckLocalBranch()->myVertices);
      contribute(CkCallback(CkReductionTarget(UnionFindLib, doneProxyCreation), thisProxy[0]));
      // contribute();
}

void UnionFindLib::
doneProxyCreation() {
  libProxyDoneCb.send();
}
#include "unionFindLib.def.h"
