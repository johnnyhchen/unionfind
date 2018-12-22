#include <iostream>
#include <assert.h>
#include "unionFindLib.h"
#include "mesh.decl.h"

/*readonly*/ CProxy_UnionFindLib libProxy;
/*readonly*/ CProxy_Main mainProxy;
/*readonly*/ int64_t MESH_SIZE;
/*readonly*/ int64_t MESHPIECE_SIZE;
/*readonly*/ float PROBABILITY;
/*readonly*/ int64_t numMeshPieces;
/*readonly*/ long batchSize;


class Map : public CkArrayMap {
  public:
    Map() {}
    inline int procNum(int, const CkArrayIndex &ind) {
      int *index=(int *) ind.data();
      return (index[0] % CkNumPes());
    }
};

class Main : public CBase_Main {
    CProxy_MeshPiece mpProxy;
    double start_time;
    uint64_t totEdges;
    uint64_t recvMPs;

    public:
    Main(CkArgMsg *m) {
        if (m->argc != 5) {
            CkPrintf("Usage: ./mesh <mesh_size> <mesh_piece_size> <probability>");
            CkExit();
        }

        MESH_SIZE = atol(m->argv[1]);
        MESHPIECE_SIZE = atol(m->argv[2]);
        if (MESH_SIZE % MESHPIECE_SIZE != 0)
            CkAbort("Invalid input: Mesh piece size must divide the mesh size!\n");
        PROBABILITY = atof(m->argv[3]);
        batchSize = atol(m->argv[4]);

        if (MESH_SIZE % MESHPIECE_SIZE != 0) {
            CkAbort("Mesh piece size should divide mesh size\n");
        }

        totEdges = 0;
        recvMPs = 0;

        numMeshPieces = (MESH_SIZE/MESHPIECE_SIZE) * (MESH_SIZE/MESHPIECE_SIZE);
        // assert ((numMeshPieces % CkNumPes()) == 0); // Finicky handling of group based union-find library
        CProxy_Map myMap = CProxy_Map::ckNew();
        CkArrayOptions opts(numMeshPieces);
        opts.setMap(myMap);
        mpProxy = CProxy_MeshPiece::ckNew(opts);
        // mpProxy = CProxy_MeshPiece::ckNew(numMeshPieces);
        // callback for library to return to after inverted tree construction
        CkCallback cb(CkIndex_Main::doneInveretdTree(), thisProxy);
        libProxy = UnionFindLib::unionFindInit(mpProxy, numMeshPieces);
        CkPrintf("[Main] Library array with %d chares created and proxy obtained\n", numMeshPieces);
        libProxy[0].register_phase_one_cb(cb);
        
        CkCallback cbB(CkIndex_MeshPiece::allowNextBatch(), mpProxy);
        libProxy[0].register_batch_cb(cbB);
        
        start_time = CkWallTimer();
        mpProxy.initializeLibVertices();
    }

    void doneInveretdTree() {
        CkPrintf("[Main] Inverted trees constructed. Notify library to do component detection\n");
        CkPrintf("[Main] Tree construction time: %f\n", CkWallTimer()-start_time);
        // mpProxy.getNumEdges();
       /* // ask the lib group chares to contribute counts
        CProxy_UnionFindLibGroup libGroup(libGroupID);
        libGroup.contribute_count();*/
        CkCallback cb(CkIndex_Main::doneFindComponents(), thisProxy);
        start_time = CkWallTimer();
        libProxy.find_components(cb);
    }

    void recvNumEdges(uint64_t numEdges)
    {
      totEdges += numEdges;
      recvMPs++;
      if (recvMPs == numMeshPieces) {
        CkPrintf("Total edges: %lld\n", totEdges);
        // CkExit();
      }
    }

    void doneFindComponents() {
        CkPrintf("[Main] Components identified, prune unecessary ones now\n");
        CkPrintf("[Main] Components detection time: %f\n", CkWallTimer()-start_time);
        CkExit();
        // callback for library to report to after pruning
        CkCallback cb(CkIndex_MeshPiece::printVertices(), mpProxy);
        libProxy.prune_components(1, cb);
    }

    void donePrinting() {
        CkPrintf("[Main] Final runtime: %f\n", CkWallTimer()-start_time);
        CkExit();
    }
};

class MeshPiece : public CBase_MeshPiece {
    MeshPiece_SDAG_CODE
    struct meshVertex {
        int64_t x,y;
        int64_t id;
        int64_t data = -1;
    };
    
    meshVertex *myVertices;
    int64_t numMyVertices;
    UnionFindLib *libPtr;
    unionFindVertex *libVertices;
    int64_t offset;
    int64_t witer;
    int64_t totalReqs;
    bool allowBatch;
    bool blockedBatch;
    uint64_t numEdges;

    public:
    MeshPiece() { 
      CkPrintf("I am chare: %d in PE: %d\n", thisIndex, CkMyPe());
    }

    MeshPiece(CkMigrateMessage *m) { }

    // function needed by library for quick lookup of
    // vertices location
    static std::pair<int64_t, int64_t> getLocationFromID(int64_t vid);
        
    void initializeLibVertices() {    
        numMyVertices = MESHPIECE_SIZE*MESHPIECE_SIZE;
        libPtr = libProxy.ckLocalBranch();
 
        // libPtr->initialize_vertices(MESHPIECE_SIZE*MESHPIECE_SIZE, libVertices, offset);
        // Initialize vertices by providing the library what the offset should be
        // The library in this case would allocate memory for all the chares in the PE by using allocate_libVertices()
        // A different usage of this library is where the application decides the offset, and allocates the vertices in the initialize_vertices() call; so offset should be passed as -1
        int64_t totalCharesinPe = numMeshPieces / CkNumPes();
        int64_t remChares = numMeshPieces % CkNumPes();
        // assert ((thisIndex / totalCharesinPe) == CkMyPe()); 
        // offset = thisIndex % totalCharesinPe;
        offset = thisIndex / CkNumPes();
        // CkPrintf("thisIndex: %d totalChareinPe: %ld offset: %ld\n", thisIndex, totalCharesinPe, offset);
        if (offset == 0) {
          // This PE gets one extra chare
          if (thisIndex < remChares)
            totalCharesinPe++;
          libProxy.ckLocalBranch()->allocate_libVertices((MESHPIECE_SIZE * MESHPIECE_SIZE), totalCharesinPe);
        }
        offset = numMyVertices * offset;
        // CkPrintf("thisIndex: %d totalChareinPe: %ld offset: %ld\n", thisIndex, totalCharesinPe, offset);
        libPtr->initialize_vertices(MESHPIECE_SIZE*MESHPIECE_SIZE, libVertices, offset, batchSize);
        libPtr->registerGetLocationFromID(getLocationFromID);
        init_vertices();

        blockedBatch = true;
        witer = 0;
        numEdges = 0;

        contribute(CkCallback(CkReductionTarget(MeshPiece, doWork), thisProxy));
    }

    void init_vertices()
    {
        try {
          myVertices = new meshVertex[MESHPIECE_SIZE*MESHPIECE_SIZE];
        }
        catch (const std::bad_alloc& ba) {
          ckout << "mem alloc error in the application: " << ba.what() << endl;
          CkExit();
        }


        //conversion of thisIndex to 2D array indices
        int64_t chare_x = thisIndex / (MESH_SIZE/MESHPIECE_SIZE);
        int64_t chare_y = thisIndex % (MESH_SIZE/MESHPIECE_SIZE);

        // populate myVertices and libVertices
        for (int64_t i = 0; i < MESHPIECE_SIZE; i++) {
            for (int64_t j = 0; j < MESHPIECE_SIZE; j++) {
                // i & j are local x & local y for the vertices here
                int64_t global_x = chare_x*MESHPIECE_SIZE + i;
                int64_t global_y = chare_y*MESHPIECE_SIZE + j;
                myVertices[i*MESHPIECE_SIZE+j].x = global_x;
                myVertices[i*MESHPIECE_SIZE+j].y = global_y;
                myVertices[i*MESHPIECE_SIZE+j].id = global_x*MESH_SIZE + global_y;
                // CkPrintf("init_vertices() id: %d\n", myVertices[i*MESHPIECE_SIZE+j].id);

                // convert global x & y to unique id for libVertices
                // retain the id decided by the application; getLocationFromID converts this id, to provide the groupIdx and offset
                libVertices[i*MESHPIECE_SIZE+j].vertexID = global_x*MESH_SIZE + global_y;
                libVertices[i*MESHPIECE_SIZE+j].parent = libVertices[i*MESHPIECE_SIZE+j].vertexID;
            }
        }
    }


    float checkProbabilityEast(int64_t val1, int64_t val2) {
        float t = ((132967*val1) + (969407*val2)) % 100;
        return t/100;
    }

    float checkProbabilitySouth(int64_t val1, int64_t val2) {
        float t = ((379721*val1) + (523927*val2)) % 100;
        return t/100;
    }

    void printVertices() {
        for (int64_t i = 0; i < numMyVertices; i++) {
            std::pair<int64_t, int64_t> loc = getLocationFromID(myVertices[i].id);
            CkPrintf("i: %d thisIndex: %d myPE: %d id: %d group: %d offset: %d\n", i, thisIndex, CkMyPe(), myVertices[i].id, loc.first, loc.second);
            //CkPrintf("[mpProxy %d] libVertices[%d] - vertexID: %ld, parent: %ld, component: %d\n", thisIndex, i, libVertices[i].vertexID, libVertices[i].parent, libVertices[i].componentNumber);
        }
        //contribute(CkCallback(CkReductionTarget(Main, donePrinting), mainProxy));
    }
    void doWork() {
      // CkPrintf("Starting doWork()\n");
      // printVertices();
      // libPtr->printVertices();
      allowBatch = false;
      totalReqs = 0;
      // continue from previous iteration
      for (; witer < numMyVertices; witer++) {
        // check probability for east edge
        float eastProb = 0.0;
        if (myVertices[witer].y + 1 < MESH_SIZE) {
          eastProb = checkProbabilityEast(myVertices[witer].y, myVertices[witer].y+1);

          if (eastProb < PROBABILITY) {
            // edge found, make library union_request call
            int64_t eastID = (myVertices[witer].x*MESH_SIZE) + (myVertices[witer].y+1);
            libPtr->union_request(myVertices[witer].id, eastID);
            totalReqs++;
          }
        }

        // check probability for south edge
        float southProb = 0.0;
        if (myVertices[witer].x + 1 < MESH_SIZE) {
          southProb = checkProbabilitySouth(myVertices[witer].x, myVertices[witer].x+1);

          if (southProb < PROBABILITY) {
            // edge found, make library union_request call
            int64_t southID = (myVertices[witer].x+1)*MESH_SIZE + myVertices[witer].y;
            libPtr->union_request(myVertices[witer].id, southID);
            totalReqs++;
          }
        }

        if (totalReqs >= batchSize) {
          numEdges += totalReqs;
          if (allowBatch == false) {
            blockedBatch = true;
            // CkPrintf("In doWork() thisIndex: %d blockedBatch: %d\n", thisIndex, blockedBatch);
            break;
          }
        }
      }
      if (witer == numMyVertices) {
        // CkPrintf("Done doWork() thisIndex: %d myPE: %d totalReqs: %ld blockedBatch: %d\n", thisIndex, CkMyPe(), totalReqs, blockedBatch);
        blockedBatch = false;
      }
    }

    void allowNextBatch() {
      allowBatch = true;
      if (blockedBatch == true) {
        // CkPrintf("In allowNextBatch() thisIndex: %d blockedBatch: %d witer: %ld numMyVertices: %ld\n", thisIndex, blockedBatch, witer, numMyVertices);
        doWork();
      }
    }

    void getNumEdges()
    {
      mainProxy.recvNumEdges(numEdges);
    }
};

std::pair<int64_t, int64_t>
MeshPiece::getLocationFromID(int64_t vid) {
    int64_t global_y = vid % MESH_SIZE;
    int64_t global_x = (vid - global_y)/MESH_SIZE;

    int64_t local_x = global_x % MESHPIECE_SIZE;
    int64_t local_y = global_y % MESHPIECE_SIZE;
    
    int64_t chare_x = (global_x-local_x) / MESHPIECE_SIZE;
    int64_t chare_y = (global_y-local_y) / MESHPIECE_SIZE;

    int64_t chareIdx = chare_x * (MESH_SIZE/MESHPIECE_SIZE) + chare_y;
    // int64_t totalCharesinPe = numMeshPieces / CkNumPes();
    // int64_t groupIdx = chareIdx / totalCharesinPe;
    int64_t groupIdx = chareIdx % CkNumPes();
    // Depending on the chareIdx, get the offset in the group
    // int64_t offset = chareIdx % totalCharesinPe;
    int64_t offset = chareIdx / CkNumPes();
    offset *= MESHPIECE_SIZE * MESHPIECE_SIZE;
    int64_t arrIdx = offset + (local_x * MESHPIECE_SIZE + local_y);

    return std::make_pair(groupIdx, arrIdx);
}

#include "mesh.def.h"
