#include <iostream>
#include <assert.h>
#include "unionFindLib.h"
#include "mesh.decl.h"

/*readonly*/ CProxy_UnionFindLib libProxy;
/*readonly*/ CProxy_Main mainProxy;
/*readonly*/ int MESH_SIZE;
/*readonly*/ int MESHPIECE_SIZE;
/*readonly*/ float PROBABILITY;
/*readonly*/ int numMeshPieces;

class Main : public CBase_Main {
    CProxy_MeshPiece mpProxy;
    double start_time;

    public:
    Main(CkArgMsg *m) {
        if (m->argc != 4) {
            CkPrintf("Usage: ./mesh <mesh_size> <mesh_piece_size> <probability>");
            CkExit();
        }

        MESH_SIZE = atoi(m->argv[1]);
        MESHPIECE_SIZE = atoi(m->argv[2]);
        if (MESH_SIZE % MESHPIECE_SIZE != 0)
            CkAbort("Invalid input: Mesh piece size must divide the mesh size!\n");
        PROBABILITY = atof(m->argv[3]);

        if (MESH_SIZE % MESHPIECE_SIZE != 0) {
            CkAbort("Mesh piece size should divide mesh size\n");
        }

        numMeshPieces = (MESH_SIZE/MESHPIECE_SIZE) * (MESH_SIZE/MESHPIECE_SIZE);
        assert ((numMeshPieces % CkNumPes()) == 0); // Finicky handling of group based union-find library
        mpProxy = CProxy_MeshPiece::ckNew(numMeshPieces);
        // callback for library to return to after inverted tree construction
        CkCallback cb(CkIndex_Main::doneInveretdTree(), thisProxy);
        libProxy = UnionFindLib::unionFindInit(mpProxy, numMeshPieces);
        CkPrintf("[Main] Library array with %d chares created and proxy obtained\n", numMeshPieces);
        libProxy[0].register_phase_one_cb(cb);
        start_time = CkWallTimer();
        mpProxy.initializeLibVertices();
    }

    void doneInveretdTree() {
        CkPrintf("[Main] Inveretd trees constructed. Notify library to do component detection\n");
        CkPrintf("[Main] Tree construction time: %f\n", CkWallTimer()-start_time);
        CkExit();
       /* // ask the lib group chares to contribute counts
        CProxy_UnionFindLibGroup libGroup(libGroupID);
        libGroup.contribute_count();*/
        CkCallback cb(CkIndex_Main::doneFindComponents(), thisProxy);
        libProxy.find_components(cb);
    }

    void doneFindComponents() {
        CkPrintf("[Main] Components identified, prune unecessary ones now\n");
        CkPrintf("[Main] Components detection time: %f\n", CkWallTimer()-start_time);
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
    struct meshVertex {
        int x,y;
        int id;
        int data = -1;
    };
    
    meshVertex *myVertices;
    int numMyVertices;
    UnionFindLib *libPtr;
    unionFindVertex *libVertices;
    long int offset;

    public:
    MeshPiece() { }

    MeshPiece(CkMigrateMessage *m) { }

    // function needed by library for quick lookup of
    // vertices location
    static std::pair<int,int> getLocationFromID(long int vid);
        
    void initializeLibVertices() {    
        numMyVertices = MESHPIECE_SIZE*MESHPIECE_SIZE;
        libPtr = libProxy.ckLocalBranch();
 
        // libPtr->initialize_vertices(MESHPIECE_SIZE*MESHPIECE_SIZE, libVertices, offset);
        // Initialize vertices by providing the library what the offset should be
        // The library in this case would allocate memory for all the chares in the PE by using allocate_libVertices()
        // A different usage of this library is where the application decides the offset, and allocates the vertices in the initialize_vertices() call; so offset should be passed as -1
        long int totalCharesinPe = numMeshPieces / CkNumPes();
        assert ((thisIndex / totalCharesinPe) == CkMyPe()); 
        offset = thisIndex % totalCharesinPe;
        // CkPrintf("thisIndex: %d totalChareinPe: %ld offset: %ld\n", thisIndex, totalCharesinPe, offset);
        if (offset == 0) {
          libProxy.ckLocalBranch()->allocate_libVertices((MESHPIECE_SIZE * MESHPIECE_SIZE), totalCharesinPe);
        }
        offset = numMyVertices * offset;
        // CkPrintf("thisIndex: %d totalChareinPe: %ld offset: %ld\n", thisIndex, totalCharesinPe, offset);
        libPtr->initialize_vertices(MESHPIECE_SIZE*MESHPIECE_SIZE, libVertices, offset);
        libPtr->registerGetLocationFromID(getLocationFromID);
        init_vertices();
        contribute(CkCallback(CkReductionTarget(MeshPiece, doWork), thisProxy));
    }

    void init_vertices()
    {
        myVertices = new meshVertex[MESHPIECE_SIZE*MESHPIECE_SIZE];

        //conversion of thisIndex to 2D array indices
        int chare_x = thisIndex / (MESH_SIZE/MESHPIECE_SIZE);
        int chare_y = thisIndex % (MESH_SIZE/MESHPIECE_SIZE);

        // populate myVertices and libVertices
        for (int i = 0; i < MESHPIECE_SIZE; i++) {
            for (int j = 0; j < MESHPIECE_SIZE; j++) {
                // i & j are local x & local y for the vertices here
                int global_x = chare_x*MESHPIECE_SIZE + i;
                int global_y = chare_y*MESHPIECE_SIZE + j;
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

    void doWork() {
        // CkPrintf("Starting doWork()\n");
        // printVertices();
        // libPtr->printVertices();
        for (int i = 0; i < numMyVertices; i++) {
            // check probability for east edge
            float eastProb = 0.0;
            if (myVertices[i].y + 1 < MESH_SIZE) {
                eastProb = checkProbabilityEast(myVertices[i].y, myVertices[i].y+1);

                if (eastProb < PROBABILITY) {
                    // edge found, make library union_request call
                    int eastID = (myVertices[i].x*MESH_SIZE) + (myVertices[i].y+1);
                    libPtr->union_request(myVertices[i].id, eastID);
                }
            }

            // check probability for south edge
            float southProb = 0.0;
            if (myVertices[i].x + 1 < MESH_SIZE) {
                southProb = checkProbabilitySouth(myVertices[i].x, myVertices[i].x+1);

                if (southProb < PROBABILITY) {
                    // edge found, make library union_request call
                    int southID = (myVertices[i].x+1)*MESH_SIZE + myVertices[i].y;
                    libPtr->union_request(myVertices[i].id, southID);
                }
            }
        }
        // CkPrintf("Done doWork() myIndex: %d myPE: %d\n", thisIndex, CkMyPe());
    }

    float checkProbabilityEast(int val1, int val2) {
        float t = ((132967*val1) + (969407*val2)) % 100;
        return t/100;
    }

    float checkProbabilitySouth(int val1, int val2) {
        float t = ((379721*val1) + (523927*val2)) % 100;
        return t/100;
    }

    void printVertices() {
        for (int i = 0; i < numMyVertices; i++) {
            std::pair<int, int> loc = getLocationFromID(myVertices[i].id);
            CkPrintf("i: %d thisIndex: %d myPE: %d id: %d group: %d offset: %d\n", i, thisIndex, CkMyPe(), myVertices[i].id, loc.first, loc.second);
            //CkPrintf("[mpProxy %d] libVertices[%d] - vertexID: %ld, parent: %ld, component: %d\n", thisIndex, i, libVertices[i].vertexID, libVertices[i].parent, libVertices[i].componentNumber);
        }
        //contribute(CkCallback(CkReductionTarget(Main, donePrinting), mainProxy));
    }
};

std::pair<int,int>
MeshPiece::getLocationFromID(long int vid) {
    int global_y = vid % MESH_SIZE;
    int global_x = (vid - global_y)/MESH_SIZE;

    int local_x = global_x % MESHPIECE_SIZE;
    int local_y = global_y % MESHPIECE_SIZE;
    
    int chare_x = (global_x-local_x) / MESHPIECE_SIZE;
    int chare_y = (global_y-local_y) / MESHPIECE_SIZE;

    int chareIdx = chare_x * (MESH_SIZE/MESHPIECE_SIZE) + chare_y;
    int totalCharesinPe = numMeshPieces / CkNumPes();
    int groupIdx = chareIdx / totalCharesinPe;
    // Depending on the chareIdx, get the offset in the group
    long int offset = chareIdx % totalCharesinPe;
    offset *= MESHPIECE_SIZE * MESHPIECE_SIZE;
    int arrIdx = offset + (local_x * MESHPIECE_SIZE + local_y);

    return std::make_pair(groupIdx, arrIdx);
}

#include "mesh.def.h"
