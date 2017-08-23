##Quorum Based Replica Control

In replicated databases, a data object has copies present at several sites. To ensure serializability, no two transactions should be allowed to read or write a data item concurrently. In case of replicated databases, a quorum-based replica control protocol can be used to ensure that no two copies of a data item are read or written by two transactions concurrently.

The quorum-based voting for replica control is due to [Gifford, 1979] Each copy of a replicated data item is assigned a vote. Each operation then has to obtain a read quorum (Vr) or a write quorum (Vw) to read or write a data item, respectively. If a given data item has a total of V votes, the quorums have to obey the following rules:

Vr + Vw > V
Vw > V/2

The first rule ensures that a data item is not read and written by two transactions concurrently. Additionally, it ensures that a read quorum contains at least one site with the newest version of the data item. The second rule ensures that two write operations from two transactions cannot occur concurrently on the same data item. The two rules ensure that one-copy serializability is maintained.

This project contains the implementation of Giffor's Quorum protocol. In addition, there is a sync operation that runs as in background thread that ensures eventual consistency of files across all replicas.

A detailed description of this implementation can be found in  "Design Document.pdf"
Instructions on how to use can be found in "User Document.pdf"

Software Requirements
- JDK
- libthrift
- slf4j
