all: kway

kway.decl.h kway.def.h: kway.ci
	charmc kway.ci

kway.o: kway.C types.h kway.decl.h kway.def.h
	charmc -c kway.C

kway: kway.o
	charmc -o kway kway.C

test:
	./kway