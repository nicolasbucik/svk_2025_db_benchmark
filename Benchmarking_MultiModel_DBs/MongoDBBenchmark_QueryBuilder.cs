using MongoDB.Bson;
using MongoDB.Driver;

public class MongoDBBenchmark_QueryBuilder
{
    private readonly IMongoDatabase _database;

    public MongoDBBenchmark_QueryBuilder() { }

    // Non-Graph Query: Find Friends of a User
    public string BuildFindFriendsDocumentQuery(string userId)
    {
        return $@"
        {{
            find: 'edges',
            filter: {{ from: '{userId}' }},
            projection: {{ to: 1, _id: 0 }}
        }}";
    }

    // Graph Query: Find Friends of a User
    public string BuildFindFriendsGraphQuery(string userId)
    {
        return $@"
        {{
            aggregate: 'edges',
            pipeline: [
                {{ $match: {{ from: '{userId}' }} }},
                {{
                    $graphLookup: {{
                        from: 'edges',
                        startWith: '$to',
                        connectFromField: 'to',
                        connectToField: 'from',
                        as: 'friends',
                        maxDepth: 0
                    }}
                }},
                {{ $unwind: '$friends' }},
                {{ $project: {{ _id: 0, to: '$friends.to' }} }}
            ],
            cursor: {{ batchSize: 10000 }}
        }}";
    }

    // Non-Graph Query: Find Friends of Friends (2-Hop Traversal)
    public string BuildFindFriendsOfFriendsDocumentQuery(string userId)
    {
        return $@"
        {{
            aggregate: 'edges',
            pipeline: [
                {{ $match: {{ from: '{userId}' }} }},
                {{
                    $lookup: {{
                        from: 'edges',
                        localField: 'to',
                        foreignField: 'from',
                        as: 'friendsOfFriends'
                    }}
                }},
                {{ $unwind: '$friendsOfFriends' }},
                {{ $project: {{ _id: 0, friendOfFriend: '$friendsOfFriends.to' }} }}
            ],
            cursor: {{ batchSize: 10000 }}
        }}";
    }

    // Graph Query: Find Friends of Friends (2-Hop Traversal)
    public string BuildFindFriendsOfFriendsGraphQuery(string userId)
    {
        return $@"
        {{
            aggregate: 'edges',
            pipeline: [
                {{ $match: {{ from: '{userId}' }} }},
                {{
                    $graphLookup: {{
                        from: 'edges',
                        startWith: '$to',
                        connectFromField: 'to',
                        connectToField: 'from',
                        as: 'friendsOfFriends',
                        maxDepth: 1,
                        depthField: 'depth'
                    }}
                }},
                {{ $unwind: '$friendsOfFriends' }},
                {{ $project: {{ _id: 0, friendOfFriend: '$friendsOfFriends.to' }} }}
            ],
            cursor: {{ batchSize: 10000 }}
        }}";
    }

    // Non-Graph Query: Find Mutual Friends Between Two Users
    public string BuildFindMutualFriendsDocumentQuery(string user1Id, string user2Id)
    {
        return $@"
        {{
            aggregate: 'edges',
            pipeline: [
                {{ $match: {{ $or: [ {{ from: '{user1Id}' }}, {{ from: '{user2Id}' }} ] }} }},
                {{ $group: {{ _id: '$to', count: {{ $sum: 1 }} }} }},
                {{ $match: {{ count: 2 }} }},
                {{ $project: {{ _id: 0, mutualFriend: '$_id' }} }}
            ],
            cursor: {{ batchSize: 10000 }}
        }}";
    }

    // Graph Query: Find Mutual Friends Between Two Users
    public string BuildFindMutualFriendsGraphQuery(string user1Id, string user2Id)
    {
        return $@"
        {{
            aggregate: 'edges',
            pipeline: [
                {{ $match: {{ from: '{user1Id}' }} }},
                {{
                    $graphLookup: {{
                        from: 'edges',
                        startWith: '$to',
                        connectFromField: 'to',
                        connectToField: 'from',
                        as: 'friends',
                        maxDepth: 0
                    }}
                }},
                {{ $unwind: '$friends' }},
                {{ $match: {{ 'friends.to': '{user2Id}' }} }},
                {{ $project: {{ _id: 0, mutualFriend: '$friends.to' }} }}
            ],
            cursor: {{ batchSize: 10000 }}
        }}";
    }

    // Non-Graph Query: Count Friends of a User
    public string BuildCountFriendsDocumentQuery(string userId)
    {
        return $@"
        {{
            count: 'edges',
            query: {{ from: '{userId}' }}
        }}";
    }

    // Graph Query: Count Friends of a User
    public string BuildCountFriendsGraphQuery(string userId)
    {
        return $@"
        {{
            aggregate: 'edges',
            pipeline: [
                {{ $match: {{ from: '{userId}' }} }},
                {{
                    $graphLookup: {{
                        from: 'edges',
                        startWith: '$to',
                        connectFromField: 'to',
                        connectToField: 'from',
                        as: 'friends',
                        maxDepth: 0
                    }}
                }},
                {{ $count: 'count' }}
            ],
            cursor: {{ batchSize: 10000 }}
        }}";
    }

    // Non-Graph Query: Find Users with More Than N Friends
    public string BuildFindUsersWithMoreThanNFriendsDocumentQuery(int n)
    {
        return $@"
        {{
            aggregate: 'edges',
            pipeline: [
                {{ $group: {{ _id: '$from', count: {{ $sum: 1 }} }} }},
                {{ $match: {{ count: {{ $gt: {n} }} }} }},
                {{ $project: {{ _id: 0, userId: '$_id' }} }}
            ],
            cursor: {{ batchSize: 10000 }}
        }}";
    }

    // Graph Query: Find Users with More Than N Friends
    public string BuildFindUsersWithMoreThanNFriendsGraphQuery(int n)
    {
        return $@"
        {{
            aggregate: 'edges',
            pipeline: [
                {{
                    $graphLookup: {{
                        from: 'edges',
                        startWith: '$from',
                        connectFromField: 'from',
                        connectToField: 'to',
                        as: 'friends',
                        maxDepth: 0
                    }}
                }},
                {{ $group: {{ _id: '$from', count: {{ $sum: 1 }} }} }},
                {{ $match: {{ count: {{ $gt: {n} }} }} }},
                {{ $project: {{ _id: 0, userId: '$_id' }} }}
            ],
            cursor: {{ batchSize: 10000 }}
        }}";
    }

    // Non-Graph Query: Find Shortest Path Between Two Users
    public string BuildFindShortestPathDocumentQuery(string startUserId, string endUserId)
    {
        return $@"
        {{
            aggregate: 'edges',
            pipeline: [
                {{ $match: {{ from: '{startUserId}' }} }},
                {{
                    $graphLookup: {{
                        from: 'edges',
                        startWith: '$to',
                        connectFromField: 'to',
                        connectToField: 'from',
                        as: 'path',
                        depthField: 'depth',
                        restrictSearchWithMatch: {{ to: '{endUserId}' }}
                    }}
                }},
                {{ $unwind: '$path' }},
                {{ $match: {{ 'path.to': '{endUserId}' }} }},
                {{ $project: {{ _id: 0, path: '$path' }} }}
            ],
            cursor: {{ batchSize: 10000 }}
        }}";
    }

    // Graph Query: Find Shortest Path Between Two Users
    public string BuildFindShortestPathGraphQuery(string startUserId, string endUserId)
    {
        return $@"
        {{
            aggregate: 'edges',
            pipeline: [
                {{ $match: {{ from: '{startUserId}' }} }},
                {{
                    $graphLookup: {{
                        from: 'edges',
                        startWith: '$to',
                        connectFromField: 'to',
                        connectToField: 'from',
                        as: 'path',
                        depthField: 'depth',
                        restrictSearchWithMatch: {{ to: '{endUserId}' }}
                    }}
                }},
                {{ $unwind: '$path' }},
                {{ $match: {{ 'path.to': '{endUserId}' }} }},
                {{ $project: {{ _id: 0, path: '$path' }} }}
            ],
            cursor: {{ batchSize: 10000 }}
        }}";
    }
}