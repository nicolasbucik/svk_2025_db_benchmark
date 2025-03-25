

namespace Benchmarking_MultiModel_DBs;

public class OrientDBBenchmark_QueryBuilder
{
    // Non-Graph Query: Find Friends of a User
    public string BuildFindFriendsDocumentQuery(string userId)
    {
        return $@"
        SELECT expand(out('edges')) 
        FROM User 
        WHERE _key = '{userId}'";
    }

    // Graph Query: Find Friends of a User
    public string BuildFindFriendsGraphQuery(string userId)
    {
        return $@"
        SELECT expand(out('edges')) 
        FROM User 
        WHERE _key = '{userId}'";
    }

    // Non-Graph Query: Find Friends of Friends (2-Hop Traversal)
    public string BuildFindFriendsOfFriendsDocumentQuery(string userId)
    {
        return $@"
        SELECT expand(out('edges').out('edges')) 
        FROM User 
        WHERE _key = '{userId}'";
    }

    // Graph Query: Find Friends of Friends (2-Hop Traversal)
    public string BuildFindFriendsOfFriendsGraphQuery(string userId)
    {
        return $@"
        SELECT expand(out('edges').out('edges')) 
        FROM User 
        WHERE _key = '{userId}'";
    }

    // Non-Graph Query: Find Mutual Friends Between Two Users
    public string BuildFindMutualFriendsDocumentQuery(string user1Id, string user2Id)
    {
        return $@"
        LET friends1 = (
            SELECT expand(out('edges')) 
            FROM User 
            WHERE _key = '{user1Id}'
        )
        LET friends2 = (
            SELECT expand(out('edges')) 
            FROM User 
            WHERE _key = '{user2Id}'
        )
        SELECT intersect(friends1, friends2) 
        FROM User";
    }

    // Graph Query: Find Mutual Friends Between Two Users
    public string BuildFindMutualFriendsGraphQuery(string user1Id, string user2Id)
    {
        return $@"
        LET friends1 = (
            SELECT expand(out('edges')) 
            FROM User 
            WHERE _key = '{user1Id}'
        )
        LET friends2 = (
            SELECT expand(out('edges')) 
            FROM User 
            WHERE _key = '{user2Id}'
        )
        SELECT intersect(friends1, friends2) 
        FROM User";
    }

    // Non-Graph Query: Count Friends of a User
    public string BuildCountFriendsDocumentQuery(string userId)
    {
        return $@"
        SELECT count(out('edges')) AS friendCount 
        FROM User 
        WHERE _key = '{userId}'";
    }

    // Graph Query: Count Friends of a User
    public string BuildCountFriendsGraphQuery(string userId)
    {
        return $@"
        SELECT count(out('edges')) AS friendCount 
        FROM User 
        WHERE _key = '{userId}'";
    }

    // Non-Graph Query: Find Users with More Than N Friends
    public string BuildFindUsersWithMoreThanNFriendsDocumentQuery(int n)
    {
        return $@"
        SELECT _key 
        FROM User 
        WHERE count(out('edges')) > {n}";
    }

    // Graph Query: Find Users with More Than N Friends
    public string BuildFindUsersWithMoreThanNFriendsGraphQuery(int n)
    {
        return $@"
        SELECT _key 
        FROM User 
        WHERE count(out('edges')) > {n}";
    }

    // Non-Graph Query: Find Shortest Path Between Two Users
    public string BuildFindShortestPathDocumentQuery(string startUserId, string endUserId)
    {
        return $@"
        SELECT shortestPath((SELECT FROM User WHERE _key = '{startUserId}'), 
                           (SELECT FROM User WHERE _key = '{endUserId}'), 'OUTBOUND')";
    }

    // Graph Query: Find Shortest Path Between Two Users
    public string BuildFindShortestPathGraphQuery(string startUserId, string endUserId)
    {
        return $@"
        SELECT shortestPath((SELECT FROM User WHERE _key = '{startUserId}'), 
                           (SELECT FROM User WHERE _key = '{endUserId}'), 'OUTBOUND')";
    }
}
