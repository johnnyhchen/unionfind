#include <iostream>
#include "unionFindLib.h"
#include "graph.decl.h"
#include "graph-io.h"


/*readonly*/ CProxy_UnionFindLib libProxy;
/*readonly*/ CProxy_Main mainProxy;
/*readonly*/ int NUM_VERTICES;
/*readonly*/ int NUM_EDGES;
/*readonly*/ int NUM_TREEPIECES;
/*readonly*/ long int lastChareBegin;

class Main : public CBase_Main {
    CProxy_TreePiece tpProxy;
    double startTime;
    public:
    // constructor for the Main class is std start point for charm program (like main())
    // argc, argv are attribute sof CkArgMsg
    Main(CkArgMsg *m) {
        if (m->argc != 3) {
            CkPrintf("Usage: ./graph <input_file> <num_chares_per_pe>\n");
            CkExit();
        }
        // read in file
        std::string inputFileName(m->argv[1]);  // parse arguments of main
        int charesPerPe = atoi(m->argv[2]);  // parse arguments of main
        FILE *fp = fopen(inputFileName.c_str(), "r");
        char line[256];
        fgets(line, sizeof(line), fp);
        line[strcspn(line, "\n")] = 0;

        std::vector<std::string> params;
        split(line, ' ', &params);

        // get params
        if (params.size() != 3) {
            CkAbort("Insufficient number of params provided in .g file\n");
        }

        NUM_VERTICES = std::stoi(params[0].substr(strlen("Vertices:")));  // number of vertices in the input graph
        NUM_EDGES = std::stoi(params[1].substr(strlen("Edges:")));  // number of edges in the input graph
        //NUM_TREEPIECES = std::stoi(params[2].substr(strlen("Treepieces:")));
        NUM_TREEPIECES = CkNumPes() * charesPerPe;  // number of compute elements

        fclose(fp);

        // need at least n vertices to have n tree pieces 
        // (but input graph not necessarily in tree structure: part of compute operation. can build tree structure
        // chare array: more generic term charm people use. different elements of the array are scattered through
        // different processing elements. they can move between processing elements too).
        if (NUM_VERTICES < NUM_TREEPIECES) {
            CkPrintf("Fewer vertices than treepieces\n");
            CkExit();
        }
        // creation of array
        // when you call ckNew the TreePiece constructor gets called on every chare element
        // TreePieces (class, def below)
        // chare array: array of TreePieces (need to initialize them when creating array)
        tpProxy = CProxy_TreePiece::ckNew(inputFileName, NUM_TREEPIECES);  
        
        // find first vertex ID on last chare
        // book keeping:
        lastChareBegin = (NUM_VERTICES/NUM_TREEPIECES) * (NUM_TREEPIECES-1) + 1;
        // create a callback for library to inform application after
        // completing inverted tree construction

        // handle that gives a method to be called when all things are done ()
        // done is a method of the main class. the main class is a chare here.
        // this proxy: tell it which main chare to call (not multiple main chares)
        // note: "Ck" is a clue you are using a charm method. 
        // ex: "CkPrintf" goes through charm library so it works.
        CkCallback cb(CkIndex_Main::done(), thisProxy);
    
        // Note: first thing you do with the UnionFindLib is here
        // call unionFindInit. give it your chare array (tpProxy) that you created on line 58
        // and the number of elements that chare array has.
        // returns this proxy. handle to a collection (likely array) of chare elements
        libProxy = UnionFindLib::unionFindInit(tpProxy, NUM_TREEPIECES);
        CkPrintf("[Main] Library array with %d chares created and proxy obtained\n", NUM_TREEPIECES);
        // 2nd thing to do using UnionFindLib: call register_phase_one_cb on the zeroeth element
        // of array chare union find just returned.
        // whatever phase 1 is, when it is done...that Main::done() method will be called
        libProxy[0].register_phase_one_cb(cb);  // call back called here
        // when you see a <name>Proxy.<method>(), that method is called on every element of the chare array
        // terminology: it's a "broadcast"
        tpProxy.initializeLibVertices();

        // libProxy[0].register_phase_one_cb(cb) and tpProxy.initializeLibVertices() :
        // not called right away. a message is queued in the runtime system
        // methods not actually run until msg is delivered to the processor the tree piece resides.
        // so you can keep doing things in this method with knowledge that the "initializeLibVerticies"
        // will not happen until we exit this entry Main() method
        // **All msg's are delivered when we exit of the method they were called from (run entire method).**
        // it's a feature: on threaded programming parigdiem: need to worry about 
        // race condition (two threads trying to read and write same data)
        // you don't have race condition because you know other methods/msg's queued up 
        // are not run until you exit this entry method. (no garuentee on which order msg/delivered methods run after they are delivered)

        // there could be race condition between two/multiple msg's that are delivered/generated by this method
        // (once this entry method exits)

        // for ex: this is Main()
        // might be some global data in Main() that you want to initialize.
        // global data: external variable you want all processors to be able to access
        // can set the global variable here (initialize/change value), knowing all msg's won't be reading it until this method finishes
        // so no msg will read the variable before we initialize it/set to proper value.
        // Gaurentee of anything done in this method will finish before the msg's are delivered and their corresponding methods run

        // global variables should only be read and not written (so initialize here in entry method)
        // charm has specific derectives: ex graph.ci
        //    readonly int NUM_TREEPIECES;
        //    readonly long lastChareBegin;
        // declared above Main(). values set in Main. other methods can read only

        // result: once you run the algorithm the .parent attribute will tell you which group (lingo: component) that vertex belongs to
    
    }

    void startWork() {
        // start time to do benchmarking (how well does this program perform). WallTimer = seconds vs. CPUTime = cycles.
        startTime = CkWallTimer();
        tpProxy.doWork();  // tp = tree piece. proxy = somethings that stands in for something else. here: array of all tree pieces. this is a broadcast to start work to all tree pieces.
    }

    // prints to console parent tree constructed
    // starts next step and defines a callback for it (what method does it call once its done)
    // (note: recall callback = method you call when a method finishes)
    void done() {
        // looks like we just called those union_requests and finished those

        CkPrintf("[Main] Inverted trees constructed. Notify library to perform components detection\n");
        CkPrintf("[Main] Tree construction time: %f\n", CkWallTimer()-startTime);
        // callback for library to inform application after completing
        // connected components detection
        CkCallback cb(CkIndex_Main::doneFindComponents(), thisProxy);
        //CkCallback cb(CkIndex_TreePiece::requestVertices(), tpProxy); //tmp, for debugging
        
        // parent tree constructed. next step: find the groups in the parent tree (each group has unique parent)
        // maybe go and find some statistics about each group (how many elements in each group for example)
        libProxy.find_components(cb);
        // when find_components done for all chares: callback doneFindComponents called
    }

    void doneFindComponents() {
        CkPrintf("[Main] Components identified, prune unecessary ones now\n");
        CkPrintf("[Main] Components detection time: %f\n", CkWallTimer()-startTime);
        // callback for library to report to after pruning
        CkCallback cb(CkIndex_TreePiece::requestVertices(), tpProxy);
        libProxy.prune_components(1, cb);  // can imagine will prune_components that i.e. have only 1 element. also reports # of components in parent tree (see UnionFindLib.C)
        // requestVertices() called when prune_components done
    }

    // end of program: does not print out for ex: totoal # of components in UnionFind datastructure
    void donePrinting() {
        CkPrintf("[Main] Final runtime: %f\n", CkWallTimer()-startTime);
        CkExit();
    }

};

// charm idiom: chare array of tree pieces. user class TreePiece is derived from class
// CBase_TreePiece. CBase_TreePiece class has all interprocessor glue in it
// allows interprocessor communication to happen when your user code just looks like an
// entry method call.
//
class TreePiece : public CBase_TreePiece {

    std::vector<proteinVertex> myVertices;  // looks like a particle (position and type fields)
    std::vector< std::pair<long int,long int> > library_requests;
    int numMyVertices;
    int numMyEdges;
    FILE *input_file;
    UnionFindLib *libPtr;
    unionFindVertex *libVertices;

    public:
    // for each chare element, read a different section of both the vertices and the edges
    // at this point, each chare element has a subset of the total vertices & a subset of the
    // edges. Random edges and vertices (not connected necessarily for a chare element).
    // point to consider: each edge is likely to link vertices that are on different processors. (keep in mind)
    TreePiece(std::string filename) {
        input_file = fopen(filename.c_str(), "r");

        /*numMyVertices = NUM_VERTICES / NUM_TREEPIECES;
        if (thisIndex == NUM_TREEPIECES - 1) {
            // last chare should get all remaining vertices if not equal division
            numMyVertices += NUM_VERTICES % NUM_TREEPIECES;
        }
        myVertices = new proteinVertex[numMyVertices];*/
        // populate myVertices
        // populateMyVertices gets passed the myVerticies vector
        // thisIndex is a charm thing: "which chare array element am I?"
        // different array elements have to do different things so that's how you distinguish
        populateMyVertices(myVertices, NUM_VERTICES, NUM_TREEPIECES, thisIndex, input_file);
        numMyVertices = myVertices.size();
        /*if (thisIndex == 0) {
            for (int i = 0; i < numMyVertices; i++)
                CkPrintf("myVertices[%d] = id: %ld\n", i, myVertices[i].id);
        }*/

        // reset input_file pointer
        fseek(input_file, 0, SEEK_SET);
        // each tree piece does not have equal number of edges (not evenly divisible. see "if" below)
        numMyEdges = NUM_EDGES / NUM_TREEPIECES;
        if (thisIndex == NUM_TREEPIECES - 1) {
            // last chare should get all remaining edges if not equal division
            numMyEdges += NUM_EDGES % NUM_TREEPIECES;
        }
        // something similar as populateMyVertices except for edges now
        // library_requests now vector of edges
        populateMyEdges(&library_requests, numMyEdges, (NUM_EDGES/NUM_TREEPIECES), thisIndex, input_file, NUM_VERTICES);
    }

    TreePiece(CkMigrateMessage *msg) { }

    // function that must be always defined by application
    // return type -> std::pair<int, int>
    // this specific logic assumes equal distribution of vertices across all tps
    static std::pair<int, int> getLocationFromID(long int vid);

    void initializeLibVertices() {
        // provide vertices data to library
        // parent can be NULL (set to -1)
        // constructs array. each array element represents a vertex.
        // in parent tree (up tree) each node has a up pointer (to parent)
        // idea: when you get an edge, find connection between vertex 1 and 2
        // you are going to represent edge in parent tree by making vertex 2's parent attribute be vertex 1
        // each group will be identified by following parent links upwards,
        // so top of given parent chain will be the label for that group
        // need to identify when you hit the top: two ways to do it: set .parent = -1 (you are top)
        // or .parent = self. (uses .parent = - 1 here. the #else would use other def).

        // this method is being run in parallel on all tree pieces at once (talk about it as broadcast from main)
        // from here
        libVertices = new unionFindVertex[numMyVertices];
        for (int i = 0; i < numMyVertices; i++) {
            libVertices[i].vertexID = myVertices[i].id;
#ifndef ANCHOR_ALGO
            libVertices[i].parent = -1;  // init all vertices to have .parent = -1
#else
            libVertices[i].parent = libVertices[i].vertexID;
#endif
        }  // to here, serial implementaion of starting up a parent tree
        // here on: we talk to union find library. using straightforward C++ library calls
        libPtr = libProxy[thisIndex].ckLocal();  // get pointer to local copy of UnionFind lib
        libPtr->initialize_vertices(libVertices, numMyVertices);  // pass array of vertices we just created to union find lib
        libPtr->registerGetLocationFromID(getLocationFromID);  // passes to UnionFindLib a method (getLocationFromID) that locates a vertex given the vertex id (returns chare vertex is on and place in the array on that chare)
        
        //  we want to execute something after all tree pieces have performed this method
        // terminology "we're doing a parallel reduction." reducing many things into one
        // sometimes you want to do arithmitic operation i.e. addition (many things sum to one)
        // here: we just want to know every thing is completed 
        // (many things: chares running methods. one thing: a counter that +=1 each time one tree piece finishes. 
        // once you get to total # of tree pieces it knows it's done). Basic idea, a lil more complicated (don't want one processor count to 1M for ex)
        // Here:
        // some function contributed as a callback
        // function in class Main called startWork. the method has to be executed by a chare 
        // (mainProxy is a handle that lets to code know where the chare sits)
        // we could be running on here i.e. processor 5000 but mainProxy might be on processor 0 i.e.
        contribute(CkCallback(CkReductionTarget(Main, startWork), mainProxy));
        // so...when all chares are finished running this method. the next thing that gets called is the startWork method by the mainProxy chare
    }
    // if I am in a chare, how do I refer to my own element in the chare array? use thisIndex
    // becuse libProxy is bound to me (a chare) I know I can get a local pointer to this element in the libProxy array (that's how I get UnionFindLib in scope)
    // via ckLocal() we can get address of UnionFindLib (if array not bound, no gaurentee where element is in parallel machine: address might be out of scope for local node)

    void doWork() {

        // vertices and edges populated, now fire union requests

        // recall library_requests is an array of edges
        for (int i = 0; i < library_requests.size(); i++) {
            // goes through all edges on this chare. sends them to union lib to process.
            // (for friends-of-friends: as you find edges you will use union_request call like this to notify UnionFindLib)
            std::pair<long int,long int> req = library_requests[i];

            // looks like we'll call these union_requests
            // and somehow it figures out when you're done calling union_request
            // then (maybe) will go to done() method (defined as a callback in main)
            libPtr->union_request(req.first, req.second);
        }
        // done() gets called when done.
    }

    // debug check: 
    void requestVertices() {
        unionFindVertex *finalVertices = libPtr->return_vertices();
        for (int i = 0; i < numMyVertices; i++) {
            //CkPrintf("[tp%d] myVertices[%d] - vertexID: %ld, parent: %ld, component: %d\n", thisIndex, i, finalVertices[i].vertexID, finalVertices[i].parent, finalVertices[i].componentNumber);
#ifndef ANCHOR_ALGO
            if (finalVertices[i].parent != -1 && finalVertices[i].componentNumber == -1) {
#else
            if (finalVertices[i].parent != finalVertices[i].vertexID && finalVertices[i].componentNumber == -1) {
#endif
                CkAbort("Something wrong in inverted-tree construction!\n");
            }
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

/*
std::pair<int, int>
TreePiece::getLocationFromID(long int vid) {
    int chareIdx = (vid-1) / (NUM_VERTICES/NUM_TREEPIECES);
    chareIdx = std::min(chareIdx, NUM_TREEPIECES-1);
    int arrIdx;
    if (vid > lastChareBegin)
        arrIdx = vid - lastChareBegin;
    else
        arrIdx = (vid-1) % (NUM_VERTICES/NUM_TREEPIECES);
    return std::make_pair(chareIdx, arrIdx);
}
*/

// vid = vertex number
//  given vertex number, locates which chare the vertex is on (chareIdx)
//  also give the array location

// I have vertex number. where on parallel machine is vertex being stored.
// need to know two numbers
// 1. which tree piece is the vertex on (arrIdx)
// 2. on that tree piece, where in the array of vertices is the vertex (for array that the tree piece holds). This is chareIdx
std::pair<int, int>
TreePiece::getLocationFromID(long int vid) {
    int chareIdx = (vid-1) % NUM_TREEPIECES;  // if tree piece reads in sequential chunk of verticies, mod it gives array number
    int arrIdx = (vid-1) / NUM_TREEPIECES;  //  runs from 0 to (num tree pieces - 1)
    return std::make_pair(chareIdx, arrIdx);
}


#include "graph.def.h"
