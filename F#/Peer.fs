module Peer
open System
open System.IO
open System.Collections
open System.Collections.Generic
open System.Linq
open MoreLinq
open System.Net
open System.Net.Sockets
open System.Reactive.Subjects
open System.Reactive.Linq
open System.Reactive.Disposables
open System.Reactive.Concurrency
open System.Reactive.Threading.Tasks
open System.Threading
open System.Threading.Tasks
open FSharp.Control.Observable
open Org.BouncyCastle.Utilities.Encoders
open FSharpx.Choice
open FSharpx.Validation
open NodaTime
open Protocol
open Db

let defaultPort = settings.ServerPort

type PeerState = Connected | Ready | Busy | Closed

type TrackerPeerState = Allocated | Ready | Busy |  Free

type GetResult<'a> = Choice<'a, exn>
let addrTopic = new Subject<Addr>()

type BloomFilterUpdate =
    | UpdateNone
    | UpdateAll
    | UpdateP2PubKeyOnly

type IPeerSend =
    abstract Receive: BitcoinMessage -> unit
    abstract Send: BitcoinMessage -> unit

type PeerQueues (stream: NetworkStream, target: IPEndPoint) = 
    let fromPeer = new Event<BitcoinMessage>()
    let toPeer = new Event<BitcoinMessage>()
    let mutable disposed = false

    interface IDisposable with
        override x.Dispose() = 
            logger.InfoF "Closing stream %A" target
            stream.Dispose()
            disposed <- true

    [<CLIEvent>]
    member x.From = fromPeer.Publish
    [<CLIEvent>]
    member x.To = toPeer.Publish

    interface IPeerSend with
        member x.Receive(message: BitcoinMessage) = if not disposed then fromPeer.Trigger message
        member x.Send(message: BitcoinMessage) = if not disposed then toPeer.Trigger message

type IPeer =
    abstract Id: int 
    abstract Ready: unit -> unit 
    abstract Bad: unit -> unit 
    abstract Target: IPEndPoint 
    abstract Receive: PeerCommand -> unit

and PeerCommand = 
    | Open of target: IPEndPoint * tip: BlockHeader
    | OpenStream of stream: NetworkStream * remote: IPEndPoint * tip: BlockHeader
    | Handshaked
    | Execute of message: BitcoinMessage
    | GetHeaders of gh: GetHeaders * task: TaskCompletionSource<IObservable<Headers>> * IPeer
    | GetBlocks of gb: GetBlocks * task: TaskCompletionSource<IPeer * IObservable<InvVector>> * IPeer
    | DownloadBlocks of gd: GetData * task: TaskCompletionSource<IPeer * IObservable<Block * byte[]>>
    | GetData of gd: GetData
    | SetReady
    | Close
    | Closed
    | UpdateScore of score: int

type TrackerCommand = 
    | GetPeers
    | Connect of target: IPEndPoint
    | IncomingConnection of stream: NetworkStream * target: IPEndPoint
    | SetReady of id: int
    | Close of int
    | BitcoinMessage of message: BitcoinMessage
    | Command of command: PeerCommand
    | SetTip of tip: BlockHeader
    | SetVersion of id: int * version: Version

type BlockchainCommand =
    | DownloadBlocks of InvEntry list * IPeerSend
    | GetHeaders of GetHeaders * IPeerSend
    | GetBlocks of GetBlocks * IPeerSend
    | Catchup of IPeer * byte[]
    | Ping of Ping * IPeerSend

type MempoolCommand =
    | Revalidate of int * seq<Tx[]>
    | Tx of Tx
    | Inv of InvVector * IPeer
    | GetTx of InvEntry list * IPeerSend
    | Mempool of IPeerSend

let blockchainIncoming = new Subject<BlockchainCommand>()
let trackerIncoming = new Subject<TrackerCommand>()
let mempoolIncoming = new Subject<MempoolCommand>()

type PartialMerkleTreeNode = 
    {
    Hash: byte[]
    Include: bool 
    Left: PartialMerkleTreeNode option 
    Right: PartialMerkleTreeNode option
    }
    override x.ToString() = sprintf "Hash(%s), Include=%b" (hashToHex x.Hash) x.Include

let scriptRuntime = new Script.ScriptRuntime(fun x _ -> x)
let checkTxAgainstBloomFilter (bloomFilter: BloomFilter) (updateMode: BloomFilterUpdate) (tx: Tx) =
    let matchScript (script: byte[]) = 
        let data = scriptRuntime.GetData script 
        data |> Array.exists (fun d -> bloomFilter.Check d) 
    let addOutpoint (txHash: byte[]) (iTxOut: int) = 
        let outpoint = new OutPoint(txHash, iTxOut)
        bloomFilter.Add (outpoint.ToByteArray())

    let matchTxHash = bloomFilter.Check tx.Hash
    let matchInput = 
        seq {
            for txIn in tx.TxIns do
                let matchTxInOutpoint = bloomFilter.Check (txIn.PrevOutPoint.ToByteArray())
                let matchTxInScript = matchScript txIn.Script
                yield matchTxInOutpoint || matchTxInScript
            } |> Seq.exists id
    let matchOutput = 
        tx.TxOuts |> Seq.mapi (fun iTxOut txOut ->
            let script = txOut.Script
            let matchTxOut = matchScript script
            if matchTxOut then 
                match updateMode with
                | UpdateAll -> addOutpoint tx.Hash iTxOut 
                | UpdateP2PubKeyOnly -> 
                    if Script.isPay2PubKey script || Script.isPay2MultiSig script then addOutpoint tx.Hash iTxOut 
                | UpdateNone -> ignore()
            matchTxOut
            ) |> Seq.exists id
    matchTxHash || matchInput || matchOutput

let buildMerkleBlock (bloomFilter: BloomFilter) (updateMode: BloomFilterUpdate) (block: Block) =
    let (txs, merkletreeLeaves) = 
        block.Txs |> Seq.map(fun tx -> 
            let txMatch = checkTxAgainstBloomFilter bloomFilter updateMode tx
            (Option.conditional txMatch tx, { Hash = tx.Hash; Include = txMatch; Left = None; Right = None })
        ) |> Seq.toList |> List.unzip

    let rec makeTree (merkletreeNodes: PartialMerkleTreeNode list) = 
        match merkletreeNodes with
        | [root] -> root 
        | _ -> 
            let len = merkletreeNodes.Length
            let nodes = 
                if len % 2 = 0 
                then merkletreeNodes
                else merkletreeNodes @ [merkletreeNodes.Last()] 
            let parentNodes = 
                    (nodes |> Seq.ofList).Batch(2) |> Seq.map (fun x ->
                    let [left; right] = x |> Seq.toList
                    let includeChildren = left.Include || right.Include
                    { 
                        Hash = dsha ([left.Hash; right.Hash] |> Array.concat) 
                        Include = includeChildren
                        Left = if includeChildren then Some(left) else None 
                        Right = if includeChildren && left <> right then Some(right) else None
                    }
                ) |> Seq.toList
            makeTree parentNodes
            
    let merkleTree = makeTree merkletreeLeaves

    let hashes = new List<byte[]>()
    let flags = new List<bool>()

    let rec depthFirstTraversal (node: PartialMerkleTreeNode) = 
        flags.Add(node.Include)
        if node.Left = None && node.Right = None then 
            hashes.Add(node.Hash) 
        node.Left |> Option.iter depthFirstTraversal
        node.Right |> Option.iter depthFirstTraversal

    depthFirstTraversal merkleTree
    let txHashes = hashes |> List.ofSeq
    let flags = new BitArray(flags.ToArray()) 
    let flagsBytes: byte[] = Array.zeroCreate ((flags.Length-1)/8+1)
    flags.CopyTo(flagsBytes, 0)
    (txs |> List.choose id, new MerkleBlock(block.Header, txHashes, flagsBytes))

type PeerData = {
    Queues: PeerQueues option
    State: PeerState
    Score: int
    CommandHandler: PeerData -> PeerCommand -> PeerData
    }

type Peer(id: int) as self = 
    let disposable = new CompositeDisposable()
    
    let mutable target: IPEndPoint = null 
    let mutable versionMessage: Version option = None 
    let mutable bloomFilterUpdateMode = BloomFilterUpdate.UpdateNone
    let mutable bloomFilter: BloomFilter option = None
    let mutable relay = 1uy
    let incoming = new Event<PeerCommand>()
    let headersIncoming = new Subject<Headers>()
    let blockIncoming = new Subject<Block * byte[]>()
    let incomingEvent = incoming.Publish
    let workLoop(stream: NetworkStream) = 
        let buffer: byte[] = Array.zeroCreate 1000000 
        let task() = stream.AsyncRead(buffer) |> Async.AsObservable
        let hasData = ref true
        Observable
            .While((fun () -> !hasData), Observable.Defer(task))
            .Do(fun c -> hasData := c > 0)
            .Where(fun c -> c > 0)
            .Select(fun c -> buffer.[0..c-1]) 
        
    let readyPeer() =
        incoming.Trigger(PeerCommand.SetReady)

    let closePeer() =
        incoming.Trigger(PeerCommand.Close)

    let badPeer() =
        incoming.Trigger(UpdateScore -100) 

    let sendMessageObs (peerQueues: PeerQueues) (message: BitcoinMessage) = 
        Observable.Defer(
            fun () -> 
                (peerQueues :> IPeerSend).Send(message)
                Observable.Empty()
            )
    let sendMessage (stream: NetworkStream) (message: BitcoinMessage) = 
        let messageBytes = message.ToByteArray()
        try
            stream.Write(messageBytes, 0, messageBytes.Length)
        with
        | e -> 
            logger.DebugF "Cannot send message to peer"
            closePeer()

    let filterMessage (message: BitcoinMessage): BitcoinMessage list =
        if relay = 1uy then
            match message.Command with
            | "tx" -> 
                let emit = 
                    bloomFilter |> Option.map (fun bf ->
                        let tx = message.ParsePayload() :?> Tx 
                        let txMatch = checkTxAgainstBloomFilter bf bloomFilterUpdateMode tx
                        if txMatch then logger.DebugF "Filtered TX %s" (hashToHex tx.Hash)
                        txMatch
                    ) |?| true
                if emit then [message] else []
            | "block" -> 
                bloomFilter |> Option.map (fun bf ->
                        let block = message.ParsePayload() :?> Block
                        let (txs, merkleBlock) = buildMerkleBlock bf bloomFilterUpdateMode block
                        let txMessages = txs |> List.map(fun tx -> new BitcoinMessage("tx", tx.ToByteArray()))
                        txs |> Seq.iter (fun tx -> logger.DebugF "Filtered TX %s" (hashToHex tx.Hash))
                        txMessages @ [new BitcoinMessage("merkleblock", merkleBlock.ToByteArray())]
                    ) |?| [message]
            | _ -> [message]
        else 
            match message.Command with
            | "tx" | "block" -> []
            | _ -> [message]

    let processMessage (peerQueues: PeerQueues) (message: BitcoinMessage) = 
        let command = message.Command
        match command with
        | "version" 
        | "verack" ->
            (peerQueues :> IPeerSend).Receive(message)
        | "getaddr" -> 
            let now = Instant.FromDateTimeUtc(DateTime.UtcNow)
            let addr = new Addr([|{ Timestamp = int32(now.Ticks / NodaConstants.TicksPerSecond); Address = NetworkAddr.MyAddress }|])
            (peerQueues :> IPeerSend).Send(new BitcoinMessage("addr", addr.ToByteArray()))
        | "getdata" ->
            let gd = message.ParsePayload() :?> GetData
            mempoolIncoming.OnNext(GetTx (gd.Invs |> List.filter (fun inv -> inv.Type = txInvType), peerQueues))
            blockchainIncoming.OnNext(DownloadBlocks (gd.Invs |> List.filter (fun inv -> inv.Type = blockInvType || inv.Type = filteredBlockInvType), peerQueues))
        | "getheaders" ->
            let gh = message.ParsePayload() :?> GetHeaders
            blockchainIncoming.OnNext(GetHeaders (gh, peerQueues))
        | "getblocks" ->
            let gb = message.ParsePayload() :?> GetBlocks
            blockchainIncoming.OnNext(GetBlocks (gb, peerQueues))
        | "addr" -> 
            let addr = message.ParsePayload() :?> Addr
            addrTopic.OnNext(addr)
        | "headers" ->
            let headers = message.ParsePayload() :?> Headers
            headersIncoming.OnNext headers
        | "block" ->
            let block = message.ParsePayload() :?> Block
            blockIncoming.OnNext (block, message.Payload)
        | "inv" ->
            let inv = message.ParsePayload() :?> InvVector
            if inv.Invs.IsEmpty then ignore() // empty inv
            elif inv.Invs.Length > 1 || inv.Invs.[0].Type <> blockInvType then // many invs or not a block inv
                mempoolIncoming.OnNext(Inv(inv, self)) // send to mempool
            elif inv.Invs.Length = 1 && inv.Invs.[0].Type = blockInvType then // a block inv, send to blockchain
                logger.DebugF "Catchup requested by %d %A %s" id self (hashToHex inv.Invs.[0].Hash)
                blockchainIncoming.OnNext(Catchup(self, inv.Invs.[0].Hash))
        | "tx" ->
            let tx = message.ParsePayload() :?> Tx
            mempoolIncoming.OnNext(Tx tx)
        | "ping" ->
            let ping = message.ParsePayload() :?> Ping // send to blockchain because tests use pings to sync with catchup
            blockchainIncoming.OnNext(BlockchainCommand.Ping(ping, peerQueues))
        | "mempool" ->
            let mempool = message.ParsePayload() :?> Mempool
            mempoolIncoming.OnNext(MempoolCommand.Mempool peerQueues)
        | "filteradd" ->
            let filterAdd = message.ParsePayload() :?> FilterAdd
            bloomFilter |> Option.iter (fun bloomFilter -> bloomFilter.Add filterAdd.Data)
            relay <- 1uy
        | "filterload" ->
            let filterLoad = message.ParsePayload() :?> FilterLoad
            let bf = new BloomFilter(filterLoad.Filter, filterLoad.NHash, filterLoad.NTweak)
            bloomFilter <- Some(bf)
            relay <- 1uy
        | "filterclear" -> 
            bloomFilter <- None
            relay <- 1uy
        | _ -> ignore()

let rec processConnection (data: PeerData) (command: PeerCommand): PeerData = 
        match command with
        | Open (t, tip) -> 
            target <- t
            logger.DebugF "Connect to %s" (target.ToString())
            let client = new Sockets.TcpClient()
            let connect = 
                async {
                    let! stream = Protocol.connect(target.Address) (target.Port)
                    return OpenStream (stream, target, tip)
                    }
            Observable.Timeout(Async.AsObservable connect, connectTimeout).Subscribe(
                onNext = (fun c -> incoming.Trigger c), 
                onError = (fun ex -> 
                    logger.DebugF "Connect failed> %A %s" t (ex.ToString())
                    (client :> IDisposable).Dispose()
                    closePeer())
            ) |> ignore
            data
        | OpenStream (stream, t, tip) -> 
            logger.DebugF "OpenStream %A" t
            target <- t
            stream.ReadTimeout <- settings.ReadTimeout
            stream.WriteTimeout <- int(commandTimeout.Ticks / TimeSpan.TicksPerMillisecond)
            let peerQueues = new PeerQueues(stream, target)
            let parser = new BitcoinMessageParser(workLoop(stream))
            peerQueues.To.SelectMany(fun m -> filterMessage m |> List.toSeq).Subscribe(onNext = (fun m -> sendMessage stream m), onError = (fun e -> closePeer())) |> ignore
            disposable.Add(peerQueues)

            let version = Version.Create(SystemClock.Instance.Now, target, NetworkAddr.MyAddress.EndPoint, int64(random.Next()), "Satoshi YOLO 1.0", tip.Height, 1uy)
            (peerQueues :> IPeerSend).Send(new BitcoinMessage("version", version.ToByteArray()))

            let handshakeObs = 
                peerQueues.From
                    .Scan((false, false), fun (versionReceived: bool, verackReceived: bool) (m: BitcoinMessage) ->
                    logger.DebugF "HS> %A" m
                    match m.Command with
                    | "version" -> 
                        let version = m.ParsePayload() :?> Version
                        versionMessage <- Some(version)
                        relay <- version.Relay
                        (peerQueues :> IPeerSend).Send(new BitcoinMessage("verack", Array.empty))
                        (true, verackReceived)
                    | "verack" -> (versionReceived, true)
                    | _ -> (versionReceived, verackReceived))
                    .SkipWhile(fun (versionReceived, verackReceived) -> not versionReceived || not verackReceived)
                    .Take(1)
                    .Select(fun _ -> Handshaked)

            Observable.Timeout(handshakeObs, handshakeTimeout).Subscribe(
                onNext = (fun c -> 
                    logger.DebugF "%A Handshaked" t
                    incoming.Trigger c),
                onError = (fun ex -> 
                    logger.DebugF "Handshake failed> %A %s" target (ex.ToString())
                    closePeer())
            ) |> ignore

            logger.DebugF "Before Subscription"
            disposable.Add(
                parser.BitcoinMessages.Subscribe(
                    onNext = (fun m -> processMessage peerQueues m), 
                    onCompleted = (fun () -> closePeer()),
                    onError = (fun e -> 
                        logger.DebugF "Exception %A" e
                        closePeer())))
            logger.DebugF "Subscription made"

            { data with Queues = Some(peerQueues) }
        | Handshaked ->
            trackerIncoming.OnNext (TrackerCommand.SetReady id)
            trackerIncoming.OnNext (TrackerCommand.SetVersion (id, versionMessage.Value))
            { data with State = Connected; CommandHandler = processCommand }
        | PeerCommand.Close -> 
            logger.DebugF "Closing %A" target
            trackerIncoming.OnNext(TrackerCommand.Close id)
            { data with CommandHandler = processClosing }
        | _ -> 
            logger.DebugF "Ignoring %A because the peer is not connected" command
            data
    and processCommand (data: PeerData) (command: PeerCommand): PeerData = 
        let peerQueues = data.Queues.Value
        match command with
        | Execute message -> 
            (peerQueues :> IPeerSend).Send(message)
            data
        | PeerCommand.GetHeaders (gh, ts, _) ->
            let sendObs = sendMessageObs peerQueues (new BitcoinMessage("getheaders", gh.ToByteArray()))
            let obs = 
                Observable
                    .Timeout(sendObs.Concat(headersIncoming), commandTimeout)
            ts.SetResult(obs)
            { data with State = PeerState.Busy }
        | PeerCommand.DownloadBlocks (gd, ts) ->
            let blocksPending = new HashSet<byte[]>(gd.Invs |> Seq.map(fun inv -> inv.Hash), new HashCompare())
            let sendObs = sendMessageObs peerQueues (new BitcoinMessage("getdata", gd.ToByteArray()))
            let count = blocksPending.Count
            let obs = 
                Observable
                    .Timeout(sendObs.Concat(blockIncoming), commandTimeout)
                    .Where(fun (b, _) -> blocksPending.Contains b.Header.Hash)
                    .Take(count)
            ts.SetResult(self :> IPeer, obs)
            { data with State = PeerState.Busy }
        | PeerCommand.SetReady -> 
            if data.State <> PeerState.Ready then
                trackerIncoming.OnNext (TrackerCommand.SetReady id)
            { data with State = PeerState.Ready }
        | GetData (gd) -> 
            let sendObs = sendMessageObs peerQueues (new BitcoinMessage("getdata", gd.ToByteArray()))
            Observable.Timeout(sendObs, commandTimeout).Subscribe(onNext = (fun _ -> ignore()), onError = (fun _ -> badPeer())) |> ignore
            data
        | UpdateScore score -> 
            let newData = { data with Score = data.Score + score }
            if newData.Score <= 0 then incoming.Trigger(PeerCommand.Close)
            newData
        | PeerCommand.Close -> 
            logger.DebugF "Closing %A" target
            trackerIncoming.OnNext(TrackerCommand.Close id)
            { data with CommandHandler = processClosing }
    and processClosing (data: PeerData) (command: PeerCommand): PeerData =
        match command with
        | Closed -> 
            (self :> IDisposable).Dispose() 
            { data with State = PeerState.Closed; Queues = None; CommandHandler = processConnection }
        | PeerCommand.GetHeaders (gh, ts, _) ->
            ts.SetResult(Observable.Empty())
            data
        | PeerCommand.DownloadBlocks (gd, ts) ->
            ts.SetResult(self :> IPeer, Observable.Empty())
            data
        | _ -> data

    let initialState = { State = PeerState.Closed; Score = 100; Queues = None; CommandHandler = processConnection }
    let runHandler (data: PeerData) (command: PeerCommand) = 
        data.CommandHandler data command
    do
        disposable.Add(
            incomingEvent
                .ObserveOn(ThreadPoolScheduler.Instance)
                .Scan(initialState, new Func<PeerData, PeerCommand, PeerData>(runHandler))
                .Subscribe())

    interface IDisposable with
        member x.Dispose() = 
            disposable.Dispose()

    interface IPeer with
        member x.Ready() = readyPeer()
        member val Id = id with get
        member x.Target with get() = target 
        member x.Bad() = badPeer()
        member x.Receive m = incoming.Trigger m

    override x.ToString() = sprintf "Peer(%d, %A, %A)" id target versionMessage

let dropOldPeers() =
    let dts = DateTime.UtcNow.AddHours(-3.0)
    Db.dropOldPeers dts
    Db.resetState()

let bootstrapPeers() =
    async {
        let now = NodaTime.Instant.FromDateTimeUtc(DateTime.UtcNow)
        let port = if settings.TestNet then 18444 else 8333

        let! entry = Async.AwaitTask(Dns.GetHostEntryAsync("seed.bitnodes.io"))
        for peer in entry.AddressList do
            let addr = { Timestamp = int (now.Ticks / NodaConstants.TicksPerSecond); Address = new NetworkAddr(new IPEndPoint(peer.MapToIPv4(), defaultPort)) }
            Db.updateAddr addr
        } |> Async.StartImmediate

let initPeers() =
    dropOldPeers()
    let peers = Db.getPeers()
    if peers.Length < 1000 then
        bootstrapPeers()
