include ../Makefile.common
all: kway

kway.decl.h kway.def.h: kway.ci
	$(CHARMC) kway.ci

kway.o: kway.C types.h kway.decl.h kway.def.h
	$(CHARMC) -c kway.C

kway: kway.o
	$(CHARMC) -o kway kway.C

test:
	./kway