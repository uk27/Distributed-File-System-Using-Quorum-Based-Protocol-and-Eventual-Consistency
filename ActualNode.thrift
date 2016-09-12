include "Node.thrift"

struct requests
	{
		//1: list<i32> path,
		1: string contents="",
		2: i32 status=0,
	}

	service NService {
		bool ping(),
		
		i32 getId (1: Node.node n),
		Node.node giveRandomNode(),
		
		requests requestRead (1: string fileName, 2: i32 nodeId) ,
		requests coordinatorFunc(1: string fileName, 2: i32 op, 3: string content),
		list<Node.node> formQuorumList(1: i32 op),
		requests readNewestVersion(1: string fileName, 2: list<Node.node> nrList),
		i32 getFileVersion(1: string fileName),
		string doRead(1: string fileName),
		list<string> getFilesOnThisNode(),
		map<i32, list <string>> getFilesOnSystem(),
		
		requests requestWrite(1: string fileName, 2: string content, 3: i32 nodeId),
		requests getHighestVersion (1: string fileName, 2: list<Node.node> nwList, 3: string content),
		i32 doWrite(1: string fileName, 2: string content),

		
	}
