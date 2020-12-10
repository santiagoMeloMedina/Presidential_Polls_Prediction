
CREATE = """CREATE {}"""

NODE = """({var}:Poll {{state:"{state}", date:date("{date}")}})"""

EDGE = """({u})-[:winner {{winner:{winner}}}]->({v})"""

INC = "incumbent"
CHAL = "challenger"

## Metrics

GDS_CREATE = """CALL gds.graph.create(
    'graph',
    'Poll',
    {
        winner: {
            orientation: 'NATURAL'
        }
    },
    {
        relationshipProperties: 'winner'
    }
)"""

LOUVAIN_SET = """CALL gds.louvain.stream('graph', { relationshipWeightProperty: 'winner' })
    YIELD nodeId, communityId
    SET gds.util.asNode(nodeId).community_id = communityId"""

LOUVAIN_CHECK = """MATCH (poll:Poll)
    RETURN poll.community_id, Count(poll.state)"""

PAGERANK = """CALL gds.pageRank.stream('graph', {
    maxIterations: 10,
    dampingFactor: 0.9,
    relationshipWeightProperty: 'winner'
    })
    YIELD nodeId, score
    RETURN score"""

DEGREE_SET = """MATCH (poll:Poll)
SET poll.degree = size((poll)-[:winner]-())"""

DEGREE_CHECK = """MATCH (poll:Poll)
RETURN size((poll)-[:winner]-()) AS degree"""