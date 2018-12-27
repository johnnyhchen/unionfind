struct findBossData {
    uint64_t arrIdx;
    uint64_t partnerOrBossID;
    uint64_t senderID;
    uint64_t isFBOne;

    void pup(PUP::er &p) {
        p|arrIdx;
        p|partnerOrBossID;
        p|senderID;
        p|isFBOne;
    }
};

#ifdef ANCHOR_ALGO
struct anchorData {
    uint64_t arrIdx;
    uint64_t v;

    void pup(PUP::er &p) {
        p|arrIdx;
        p|v;
    }
};
#endif

struct needRootData {
  int64_t req_vertex;
  int64_t parent_arrID;

  void pup(PUP::er &p) {
    p|req_vertex;
    p|parent_arrID;
  }
};

struct shortCircuitData {
    uint64_t arrIdx;
    uint64_t grandparentID;

    void pup(PUP::er &p) {
        p|arrIdx;
        p|grandparentID;
    }
};
