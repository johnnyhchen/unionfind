#include <iostream>
#include <vector>
#include <string>
#include <sstream>

// vertex structure for given graph
struct proteinVertex {
    long int id;
    char complexType;
    float x,y,z;
};

void seekToLine(FILE *fp, long int lineNum);
void populateMyVertices(proteinVertex* myVertices, int nMyVertices, int chareIdx, FILE *fp);
void split(char *str, char delim, std::vector<std::string> *elems);
proteinVertex ReadVertex(FILE *fp);
std::pair<long int, long int> ReadEdge(FILE *fp);
void populateMyEdges(std::vector< std::pair<long int, long int> > *myEdges, int nMyEdges, int chareIdx, FILE *fp, int totalNVertices);

int main() {
    // like the constructor of treePiece
    FILE *input_file = fopen("pdb7icg.g", "r");
    proteinVertex *myVertices = new proteinVertex[99];
    populateMyVertices(myVertices, 5, 0, input_file);
    for(int i = 0; i < 5; i++) {
        printf("Vertex %d:\n", i);
        printf("id: %ld, complex: %c, x: %f, y: %f, z: %f\n", myVertices[i].id, myVertices[i].complexType, myVertices[i].x, myVertices[i].y, myVertices[i].z);
    }

    // reset input_file
    fseek(input_file, 0, SEEK_SET);

    std::vector< std::pair<long int,long int> > myEdges;
    populateMyEdges(&myEdges, 5, 28, input_file, 2871);
    for (int i = 0; i < 5; i++) {
        printf("Edge %d:\n", i);
        printf("from: %ld, to: %ld\n", myEdges[i].first, myEdges[i].second);
    }

    return 0;
}

void populateMyVertices(proteinVertex* myVertices, int nMyVertices, int chareIdx, FILE *fp) {
    int startVid = (nMyVertices * chareIdx) + 1;
    long int lineNum = startVid + 2;
    seekToLine(fp, lineNum);
    for (int i = 0; i < nMyVertices; i++) {
        myVertices[i] = ReadVertex(fp);
    }
}

void populateMyEdges(std::vector< std::pair<long int, long int> > *myEdges, int nMyEdges, int chareIdx, FILE *fp, int totalNVertices) {
   int startEid = (nMyEdges * chareIdx) + 1;
   long int lineNum = startEid + totalNVertices + 2;
   printf("lineNum : %ld\n", lineNum);
   seekToLine(fp, lineNum);
   for (int i = 0; i < nMyEdges; i++) {
       std::pair<long int, long int> edge = ReadEdge(fp);
       myEdges->push_back(edge);
   }
}

void seekToLine(FILE *fp, long int lineNum) {
    long int i = 1, byteCount = 0;
    if (fp != NULL) {
        char line[256];
        while (fgets(line, sizeof(line), fp) != NULL) {
            if (i == lineNum) {
                break;
            }
            else {
                i++;
                byteCount += strlen(line);
            }
        }
        // seek to byteCount
        fseek(fp, byteCount, SEEK_SET);
    }
    else {
        printf("File not found\n");
    }
}

void split(char *str, char delim, std::vector<std::string> *elems) {
    std::stringstream ss;
    ss << (char*) str;
    std::string item;
    while(getline(ss, item, delim)) {
        elems->push_back(item);
    }
}

void sanitizeFields(std::vector<std::string> *fields) {
    std::vector<std::string>::iterator iter;
    for (iter = fields->begin(); iter != fields->end();) {
        if (iter->find_first_not_of(' ') == std::string::npos) {
            iter = fields->erase(iter);
        }
        else {
            iter++;
        }
    }
    iter = fields->end();
    iter->pop_back();
}

proteinVertex ReadVertex(FILE *fp) {
    char line[256];
    fgets(line, sizeof(line), fp);
    line[strcspn(line, "\n")] = 0; // removes trailing newline character
    std::vector<std::string> vertexFields;
    split(line, ' ', &vertexFields);
    sanitizeFields(&vertexFields);

    if (vertexFields.size() != 7 || vertexFields[0] != "v") {
        printf("Error in vertex format\n");
        exit(0);
    }

    proteinVertex newVertex;
    newVertex.id = std::stol(vertexFields[1]);
    newVertex.complexType = vertexFields[2][0];
    newVertex.x = std::stof(vertexFields[4]);
    newVertex.y = std::stof(vertexFields[5]);
    newVertex.z = std::stof(vertexFields[6]);

    return newVertex;
}

std::pair<long int, long int> ReadEdge(FILE *fp) {
    char line[256];
    fgets(line, sizeof(line), fp);
    line[strcspn(line, "\n")] = 0;

    std::vector<std::string> edgeFields;
    split(line, ' ', &edgeFields);

    if (edgeFields.size() != 4 || edgeFields[0] != "u") {
        printf("Error in edge format\nObtained edge: %s\n", line);
        exit(0);
    }

    std::pair<long int,long int> newEdge;
    newEdge.first = std::stol(edgeFields[1]);
    newEdge.second = std::stol(edgeFields[2]);

    return newEdge;
}


//int ReadToken(char *token, FILE *fp, long int *pLineNo)
