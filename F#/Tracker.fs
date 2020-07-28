module Tracker
open System
open System.Net
open System.Net.Sockets
open System.Collections.Generic
open System.IO
open System.Text
open System.Threading
open System.Reactive.Linq
open System.Reactive.Subjects
open System.Reactive.Disposables
open System.Reactive.Concurrency
open System.Threading.Tasks
open FSharpx.Option
open FSharpx.Validation
open Protocol
open Peer

type PeerSlot = {
    Id: int
    Peer: Peer
    mutable State: TrackerPeerState
    }

let maxSlots = settings.MaxPeers

let mutable connectionCount = 0 
let mutable seqId = 0 
let disposable = new CompositeDisposable()
let scheduler = new EventLoopScheduler()
let incomingMessage = new Subject<BitcoinMessage>()
let broadcastToPeers = new Subject<BitcoinMessage>()
let pendingMessages = new Queue<TrackerCommand>()

let nextId() = 
    seqId <- seqId + 1
    seqId

let mutable peerSlots = Map.empty<int, PeerSlot>
exception InvalidTransition of TrackerPeerState * TrackerPeerState

let checkTransition oldState newState = 
    match (oldState, newState) with
    | (Allocated, Ready) |
        (_, Free) |
        (Ready, Busy) |
        (Busy, Ready) -> ignore()
    | other -> 
        logger.ErrorF "Invalid transition from %A to %A" oldState newState
        raise (InvalidTransition other)

let changeState id newState =
    let oldState = peerSlots.[id].State
    let peer: IPeer = peerSlots.[id].Peer :> IPeer
    logger.DebugF "State transition of peer(%d %A): %A->%A" id peer.Target oldState newState
    checkTransition oldState newState
    peerSlots.[id].State <- newState
    oldState

let dequeuePendingMessage() = 
    logger.DebugF "Dequeue Msgs"
    if pendingMessages.Count <> 0 then
        trackerIncoming.OnNext(pendingMessages.Dequeue())
    else
        logger.DebugF "Empty Queue"

let assign message ps = 
    let peer = ps.Peer :> IPeer
    logger.DebugF "Assigning %A to %A" message peer.Target
    changeState ps.Id Busy |> ignore
    peer.Receive(message)

let forward (command: TrackerCommand) (message: PeerCommand) =
    match message with
        | PeerCommand.GetHeaders(_, ts, peer) -> 
            peerSlots |> Map.tryFind peer.Id
            |> Option.bind (fun ps -> Option.conditional (ps.State = Ready) ps) |> Option.map (fun ps -> assign message ps)
            |> getOrElseF (fun () -> ts.SetResult(Observable.Throw(ArgumentException("Peer busy - cannot handle command"))))
        | _ -> 
            peerSlots |> Map.tryPick(fun _ ps -> if ps.State = Ready then Some(ps) else None)
            |> Option.map (fun ps -> assign message ps)
            |> getOrElseF (fun () ->
                let freePeers = peerSlots |> Seq.filter(fun ps -> ps.Value.State = Free) |> Seq.length
                logger.DebugF "No peer available - %d dead peers" freePeers
                pendingMessages.Enqueue command // park the command for now
                )

let newPeer() = 
    let openSlotId = nextId()
    peerSlots <- peerSlots.Add(openSlotId, { Id = openSlotId; Peer = new Peer(openSlotId); State = Allocated })
    let peer = peerSlots.[openSlotId].Peer
    connectionCount <- connectionCount + 1
    peer

let freePeer (id: int) =
    peerSlots |> Map.tryFind id |> Option.iter (fun ps -> 
        logger.DebugF "Freeing %A" (peerSlots.[id].Peer :> IPeer).Target
        changeState id Free |> ignore
        let peer =  ps.Peer
        connectionCount <- connectionCount - 1
        if connectionCount < maxSlots then
            Db.updateState((peer :> IPeer).Target, -1) // blackball this peer
            let newPeer = Db.getPeer() // find a new peer
            newPeer |> Option.iter (fun p -> 
                Db.updateState(p, 1)
                trackerIncoming.OnNext(Connect p))
        peerSlots <- peerSlots.Remove id
        (peer :> IPeer).Receive Closed // tell the old peer to take a hike
    )

let mutable tip = Db.readHeader (Db.readTip())
let processCommand(command: TrackerCommand) =
    logger.DebugF "TrackerCommand> %A" command
    match command with
    | SetTip t -> 
        tip <- t
        Db.pruneBlocks (tip.Height-settings.PruneDepth)
    | GetPeers -> 
        let peers = Db.getPeers()
        let cFreeSlots = maxSlots - peerSlots.Count
        for peer in peers |> Seq.truncate cFreeSlots do
            Db.updateState(peer, 1)
            trackerIncoming.OnNext(Connect peer)
    | Connect target ->
        let peer = newPeer() :> IPeer
        peer.Receive(Open(target, tip))
    | IncomingConnection (stream, target) ->
        let peer = newPeer() :> IPeer
        peer.Receive(OpenStream(stream, target, tip))
    | SetReady id ->
        peerSlots.TryFind id |> Option.iter(fun ps ->
            let peer = ps.Peer
            let oldState = changeState id Ready
            if oldState = Allocated then 
                logger.InfoF "Connected to %A" peer
            dequeuePendingMessage()
            )
    | SetVersion (id, version) ->
        peerSlots.TryFind id |> Option.iter(fun ps ->
            if version.Height > tip.Height then blockchainIncoming.OnNext(Catchup(ps.Peer, null))
            )
    | Close id -> 
        freePeer id
        logger.DebugF "Connection count = %d" connectionCount
    | BitcoinMessage message -> forward command (Execute message)
    | Command peerCommand -> forward command peerCommand

let processAddr (addr: Addr) =
    for a in addr.Addrs do Db.updateAddr(a)

let getHeaders(gh: GetHeaders, peer: IPeer): Task<IObservable<Headers>> = 
    let ts = new TaskCompletionSource<IObservable<Headers>>()
    trackerIncoming.OnNext(Command (PeerCommand.GetHeaders (gh, ts, peer)))
    ts.Task

let getBlocks(blockHashes: seq<byte[]>): Task<IPeer * IObservable<Block * byte[]>> =
    let invHashes = 
        seq { 
            for h in blockHashes do
                yield new InvEntry(blockInvType, h) }
        |> Seq.toList
    let gd = new GetData(invHashes)
    let ts = new TaskCompletionSource<IPeer * IObservable<Block * byte[]>>()
    trackerIncoming.OnNext(Command (PeerCommand.DownloadBlocks (gd, ts)))
    ts.Task

let processBroadcast (m: BitcoinMessage) = 
    for peerSlot in peerSlots do
        (peerSlot.Value.Peer :> IPeer).Receive(Execute m)

let startTracker() =
    disposable.Add(trackerIncoming.ObserveOn(scheduler).Subscribe(processCommand))
    disposable.Add(incomingMessage.Select(fun m -> BitcoinMessage m).Subscribe(trackerIncoming))
    disposable.Add(Peer.addrTopic.ObserveOn(scheduler).Subscribe(processAddr))
    disposable.Add(broadcastToPeers.ObserveOn(scheduler).Subscribe(processBroadcast))

let startServer() = 
    let port = defaultPort
    let ipAddress = IPAddress.Any
    let endpoint = IPEndPoint(ipAddress, port)
    let tcpListener = new TcpListener(endpoint)
    let listener = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp)
    tcpListener.Start()
    logger.InfoF "Started listening on port %d" port
    let rec accept() = 
        async {
            let! socket = Async.FromBeginEnd(tcpListener.BeginAcceptSocket, tcpListener.EndAcceptSocket)
            let stream = new NetworkStream(socket, true)
            trackerIncoming.OnNext(TrackerCommand.IncomingConnection(stream, socket.RemoteEndPoint :?> IPEndPoint))
            return! accept()
            }

    accept() |> Async.Start
