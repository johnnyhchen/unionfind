include ../../Makefile.common

all: graph-omp

graph-seq: graph.o
	$(CHARMC) ${LD_OPTS} -o $@ graph.o ${UNION_FIND_LIBS} -lgraph_generator_seq -L./generator

graph-omp: graph.o
	$(CHARMC) ${LD_OPTS} -fopenmp -o $@ graph.o ${UNION_FIND_LIBS} -lgraph_generator_omp -L./generator

graph.o : graph.C graph.decl.h graph.def.h vtype.h
	$(CHARMC) -c ${OPTS} ${UNION_FIND_INC} -I./generator/include $(GRAPHGEN_DEFS) graph.C -I$(MPI_ROOT)/include

graph.decl.h graph.def.h : graph.ci
	$(CHARMC) -E graph.ci

clean:
	rm -f *.decl.h *.def.h conv-host *.o graph-seq graph-omp charmrun
	rm -f obtained.*

cleanp:
	rm -f *.sts *.gz *.projrc *.topo *.out

cleanpl:
	rm -f *.sts *.gz *.projrc *.topo *.out *.log

test-all: all
	./run_tests
