#ifndef GRAPH_IO_H_
#define GRAPH_IO_H_

#include <fstream>

#define METADATA_LINES 3

void writeMetadata(std::ofstream& out_fs, const int64_t num_vertices,
                   const int64_t num_edges) {
  out_fs << "Kronecker Graph" << std::endl;
  out_fs << num_vertices << " " << num_edges << std::endl;
  out_fs << "--- End Metadata ---" << std::endl;
}

#endif // GRAPH_IO_H_
