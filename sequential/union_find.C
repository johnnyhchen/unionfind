#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <string>
#include <utility>
#include <vector>
#include <map>
#include "../examples/simple_graph/graph-io.h"
using namespace std;

struct libVertex {
    int id;
    int parent; // id of parent vertex
    int component;
};

// global array of library vertices
libVertex *vertices;

// function to get array index from vertex id
// provided by application
int getArrayIndex(int vid) {
    // trivial case where id = index in array
    return vid-1;
}

// function to make sets
void make_set(int idx, int x) {
    vertices[idx].parent = -1;
    vertices[idx].id = x;
    vertices[idx].component = -1;
}

// function to find boss of a queried vertex
// return id of the boss
int find(int vid) {
    int arrIdx = getArrayIndex(vid);
    while (vertices[arrIdx].parent != -1) {
        arrIdx = getArrayIndex(vertices[arrIdx].parent);
    }

    return vertices[arrIdx].id;
}

// function to handle Union requests in simple manner
// make boss of vid2 point to boss of vid1
// no size or height consideration
void union_simple(int vid1, int vid2) {
    int b1 = find(vid1);
    int b2 = find(vid2);
    if (b1 != b2) { //self-loop check for cycles
        vertices[getArrayIndex(b2)].parent = b1;
    }
}

// sequential function to set component
void find_components(int vid) {
    int rootId = find(vid);
    vertices[getArrayIndex(vid)].component = rootId;
}


int main (int argc, char **argv) {

    if (argc < 4) {
        printf("Usage: ./union_find <input_file> <num_vertices> <num_edges>\n");
        exit(0);
    }

    string inputFileName(argv[1]);
    int numVertices = atoi(argv[2]);
    int numEdges = atoi(argv[3]);

    FILE *fp = fopen(inputFileName.c_str(), "r");

    proteinVertex *myVertices = new proteinVertex[numVertices];
    populateMyVertices(myVertices, numVertices, numVertices, 0, fp);

    vertices = new libVertex[numVertices];
    for (int i = 0; i < numVertices; i++) {
        make_set(i, myVertices[i].id);
    }

    vector< pair<long int,long int> > unionRequests;
    fseek(fp, 0, SEEK_SET);
    populateMyEdges(&unionRequests, numEdges, numEdges, 0, fp, numVertices);

    for (int i = 0; i < unionRequests.size(); i++) {
        union_simple(unionRequests[i].first, unionRequests[i].second);
    }
    
    printf("Started find_components\n");

    map<int,int> component_map;
    for (int i = 0; i < numVertices; i++) {
        find_components(myVertices[i].id);
        component_map[vertices[i].component]++;
    }

    for (int i = 0; i < numVertices; i++) {
        printf("Vertices[%d]: id=%d, parent=%d, component=%d\n", i, vertices[i].id, vertices[i].parent, vertices[i].component);
    }

    map<int,int>::iterator it = component_map.begin();
    while (it != component_map.end()) {
        printf("Component %d : Total vertices count = %d\n", it->first, it->second);
        it++;
    }

    return 0;

    /*std::vector< std::pair<int, int> > union_requests;
    
    for (int i = 0; i < union_requests.size(); i++) {
        std::pair<int, int> req = union_requests[i];
        union_simple(req.first, req.second);
    }

    for (int i = 0; i < NUM_VERTICES; i++)
        printf("Vertices[%d] : id=%d, parent=%d\n", i, vertices[i].id, vertices[i].parent);

    return 0;*/
}
