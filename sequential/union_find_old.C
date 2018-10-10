#include <iostream>
#include <utility>
#include <vector>
using namespace std;

#define NUM_VERTICES 9

struct vertex {
    int id;
    int parent; // id of parent vertex
}vertices[NUM_VERTICES];

// function to get array index from vertex id
// provided by application
int getArrayIndex(int vid) {
    // trivial case where id = index in array
    return vid;
}

// function to make sets
void make_set(int x) {
    vertices[x].parent = -1;
    vertices[x].id = x;
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
    vertices[getArrayIndex(b2)].parent = b1;
}


int main (int argc, char **argv) {

    for (int i = 0; i < NUM_VERTICES; i++)
        make_set(i);

    std::vector< std::pair<int, int> > union_requests;
    union_requests.push_back(std::make_pair(3, 0));
    union_requests.push_back(std::make_pair(3, 2));
    union_requests.push_back(std::make_pair(1, 4));
    union_requests.push_back(std::make_pair(2, 4));
    union_requests.push_back(std::make_pair(5, 6));
    union_requests.push_back(std::make_pair(8, 7));
    union_requests.push_back(std::make_pair(5, 8));
    union_requests.push_back(std::make_pair(4, 7));


    for (int i = 0; i < union_requests.size(); i++) {
        std::pair<int, int> req = union_requests[i];
        union_simple(req.first, req.second);
    }

    for (int i = 0; i < NUM_VERTICES; i++)
        printf("Vertices[%d] : id=%d, parent=%d\n", i, vertices[i].id, vertices[i].parent);

    return 0;
}
