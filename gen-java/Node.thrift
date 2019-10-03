service NodeService {
            bool ping(),
            string readFromServer(1: string fileName, 2: i32 passedServers),
            string writeToServer(1: string fileName, 2: i32 passedServers),
            bool joinCoordinator(1: string coordinatorIp, 2: i32 coordinatorPort, 3: string ip, 4: i32 port),
            void updateCoordinatorList(1: string ip, 2: i32 port),
            list<string> requestQuorum(1: string type, 2: string ip, 3: i32 port, 4: string fileName, 5: i32 passedServers),
            i32 getVersionNumber(1: string fileName),
            void writeComplete(1: string fileName),
            bool writeToFileSystem(1: string fileName),
            void readComplete(),
            string readFromFileSystem(1: string fileName),
            map<string, i32> synchGet(),
            void synchPut(1: map<string, i32> updatedVersions)
}
