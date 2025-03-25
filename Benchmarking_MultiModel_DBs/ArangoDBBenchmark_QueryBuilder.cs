public class ArangoDBBenchmark_QueryBuilder
{
    // Non-Graph Query: Find Friends of a User
    public string BuildFindFriendsDocumentQuery(string userId)
    {
        return $@"
        FOR edge IN edges
            FILTER edge._from == '{userId}'
            RETURN edge._to";
    }

    // Graph Query: Find Friends of a User
    public string BuildFindFriendsGraphQuery(string userId)
    {
        return $@"
        FOR vertex, edge IN 1..1 OUTBOUND '{userId}' edges
            RETURN vertex._key";
    }

    // Non-Graph Query: Find Friends of Friends (2-Hop Traversal)
    public string BuildFindFriendsOfFriendsDocumentQuery(string userId)
    {
        return $@"
        LET friends = (
            FOR edge IN edges
                FILTER edge._from == '{userId}'
                RETURN edge._to
        )
        FOR friend IN friends
            FOR edge IN edges
                FILTER edge._from == friend
                RETURN edge._to";
    }

    // Graph Query: Find Friends of Friends (2-Hop Traversal)
    public string BuildFindFriendsOfFriendsGraphQuery(string userId)
    {
        return $@"
        FOR vertex, edge, path IN 2..2 OUTBOUND '{userId}' edges
            RETURN vertex._key";
    }

    // Non-Graph Query: Find Mutual Friends Between Two Users
    public string BuildFindMutualFriendsDocumentQuery(string user1Id, string user2Id)
    {
        return $@"
        LET friends1 = (
            FOR edge IN edges
                FILTER edge._from == '{user1Id}'
                RETURN edge._to
        )
        LET friends2 = (
            FOR edge IN edges
                FILTER edge._from == '{user2Id}'
                RETURN edge._to
        )
        FOR friend IN INTERSECTION(friends1, friends2)
            RETURN friend";
    }

    // Graph Query: Find Mutual Friends Between Two Users
    public string BuildFindMutualFriendsGraphQuery(string user1Id, string user2Id)
    {
        return $@"
        LET friends1 = (
            FOR vertex, edge IN 1..1 OUTBOUND '{user1Id}' edges
                RETURN vertex._key
        )
        LET friends2 = (
            FOR vertex, edge IN 1..1 OUTBOUND '{user2Id}' edges
                RETURN vertex._key
        )
        FOR friend IN INTERSECTION(friends1, friends2)
            RETURN friend";
    }

    // Non-Graph Query: Count Friends of a User
    public string BuildCountFriendsDocumentQuery(string userId)
    {
        return $@"
        RETURN LENGTH(
            FOR edge IN edges
                FILTER edge._from == '{userId}'
                RETURN edge._to
        )";
    }

    // Graph Query: Count Friends of a User
    public string BuildCountFriendsGraphQuery(string userId)
    {
        return $@"
        RETURN LENGTH(
            FOR vertex, edge IN 1..1 OUTBOUND '{userId}' edges
                RETURN vertex._key
        )";
    }

    // Non-Graph Query: Find Users with More Than N Friends
    public string BuildFindUsersWithMoreThanNFriendsDocumentQuery(int n)
    {
        return $@"
    FOR edge IN edges
        COLLECT from = edge._from INTO groups
        LET count = LENGTH(groups)
        FILTER count > {n}
        RETURN from";
    }

    // Graph Query: Find Users with More Than N Friends
    public string BuildFindUsersWithMoreThanNFriendsGraphQuery(int n)
    {
        return $@"
        LET friends = (
            FOR vertex, edge IN 1..1 OUTBOUND 'users/{{userId}}' edges
                RETURN edge._from
        )
        FOR friend IN friends
            COLLECT from = friend INTO groups
            LET count = LENGTH(groups)
            FILTER count > {n}
            RETURN from";
    }

    // Non-Graph Query: Find Shortest Path Between Two Users
    public string BuildFindShortestPathDocumentQuery(string startUserId, string endUserId)
    {
        return $@"
        FOR path IN OUTBOUND SHORTEST_PATH '{startUserId}' TO '{endUserId}' edges
            RETURN path";
    }

    // Graph Query: Find Shortest Path Between Two Users
    public string BuildFindShortestPathGraphQuery(string startUserId, string endUserId)
    {
        return $@"
        FOR vertex, edge IN OUTBOUND SHORTEST_PATH '{startUserId}' TO '{endUserId}' edges
            RETURN vertex._key";
    }
}