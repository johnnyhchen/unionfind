#ifndef VTYPE_H_
#define VTYPE_H_

#include <string>

#define USE_PROTEIN
//#define USE_SAMPLE

// vertex structure for given graph
#ifdef USE_PROTEIN
struct proteinVertex {
    long int id;
    char complexType;
    float x,y,z;
};
#endif

#ifdef USE_SAMPLE
struct proteinVertex {
    long int id;
    std::string type;
};
#endif

#endif // VTYPE_H_
