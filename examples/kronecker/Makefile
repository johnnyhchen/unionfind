include ../../Makefile.common

all: graph

graph: graph.o
	$(CHARMC) ${LD_OPTS} -o $@ graph.o ${UNION_FIND_LIBS}

graph.o : graph.C graph.decl.h graph.def.h vtype.h
	$(CHARMC) -c ${OPTS} ${UNION_FIND_INC} -I./graph500-gen -DGRAPH_GENERATOR_MPI graph.C -I$(MPI_ROOT)/include

graph.decl.h graph.def.h : graph.ci
	$(CHARMC) -E graph.ci

clean:
	rm -f *.decl.h *.def.h conv-host *.o graph charmrun
	rm -f obtained.*

cleanp:
	rm -f *.sts *.gz *.projrc *.topo *.out

cleanpl:
	rm -f *.sts *.gz *.projrc *.topo *.out *.log

test-all: all
	./run_tests
