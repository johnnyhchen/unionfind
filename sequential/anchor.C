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
    vertices[idx].parent = x;
    vertices[idx].id = x;
    vertices[idx].component = -1;
}

// function to perform Anchor for a union(v,w)
void anchor(int v, int w) {
    if (vertices[getArrayIndex(w)].parent == v)
        return;

    if (w < v) {
        anchor(vertices[getArrayIndex(w)].parent, v);
    }
    else if (vertices[getArrayIndex(w)].parent == w) {
        vertices[getArrayIndex(w)].parent = v;
    }
    else {
        anchor(v, vertices[getArrayIndex(w)].parent);
    }
}

// function to find boss of a queried vertex
// return id of the boss
int find(int vid) {
    int arrIdx = getArrayIndex(vid);
    while (vertices[arrIdx].parent != vertices[arrIdx].id) {
        arrIdx = getArrayIndex(vertices[arrIdx].parent);
    }

    return vertices[arrIdx].id;
}

// function to handle Union requests in simple manner
// make boss of vid2 point to boss of vid1
// no size or height consideration
void union_simple(int vid1, int vid2) {
    anchor(vid1, vid2);
}

// sequential function to set component
void find_components(int vid) {
    int rootId = find(vid);
    vertices[getArrayIndex(vid)].component = rootId;
}


int main (int argc, char **argv) {

    if (argc < 2) {
        printf("Usage: ./anchor <input_file>\n");
        exit(0);
    }

    string inputFileName(argv[1]);

    FILE *fp = fopen(inputFileName.c_str(), "r");
    char line[256];
    fgets(line, sizeof(line), fp);
    line[strcspn(line, "\n")] = 0;

    std::vector<std::string> params;
    split(line, ' ', &params);

    int numVertices = std::stoi(params[0].substr(strlen("Vertices:")));
    int numEdges = std::stoi(params[1].substr(strlen("Edges:")));

    fclose(fp);

    fp = fopen(inputFileName.c_str(), "r");

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
        //printf("Vertices[%d]: id=%d, parent=%d, component=%d\n", i, vertices[i].id, vertices[i].parent, vertices[i].component);
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
