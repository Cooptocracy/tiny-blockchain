module Mempool
open System
open System.Linq
open System.Collections.Generic
open System.Reactive.Linq
open System.Reactive.Concurrency
open FSharpx
open FSharpx.Collections
open FSharpx.Choice
open FSharpx.Validation
open Protocol
open Db
open Peer
open Tracker

let mutable listTx = new List<byte[]>()
let mutable mempool = new Dictionary<byte[], Tx>(new HashCompare())
let mutable mempoolHeight = 0

type NopUndoWriter() =
    interface IUTXOWriter with
        member x.Write(_: TxOperation, _: OutPoint, _: UTXO) = ignore()

type OutpointCompare() =
    interface IEqualityComparer<OutPoint> with
        member x.Equals(left: OutPoint, right: OutPoint) =
            if obj.ReferenceEquals(left, right) then true
            else
                hashCompare.Equals(left.Hash, right.Hash) && left.Index = right.Index

        member x.GetHashCode(outpoint: OutPoint) = hashCompare.GetHashCode(outpoint.Hash)

type MempoolUTXOAccessor(baseUTXO: IUTXOAccessor) =
    let table = new Dictionary<OutPoint, UTXO>(new OutpointCompare())
    let counts = new Dictionary<byte[], int>(new HashCompare()) 
    let getOrDefault (hash: byte[]) = 
        let (f, count) = counts.TryGetValue(hash) 
        if f then count else 0
    let deleteUTXO (outpoint: OutPoint) = 
        table.[outpoint] <- null
        let count = getOrDefault outpoint.Hash
        counts.[outpoint.Hash] <- count-1
    let addUTXO (outpoint: OutPoint) (utxo: UTXO) = 
        table.[outpoint] <- utxo
        let count = getOrDefault outpoint.Hash
        counts.[outpoint.Hash] <- count+1
        
    let getUTXO (outpoint: OutPoint) =
        let (f, utxo) = table.TryGetValue(outpoint)
        if f
        then 
            if utxo <> null 
            then Some(utxo)
            else 
                logger.DebugF "Cannot find outpoint %A" outpoint
                None
        else
            baseUTXO.GetUTXO outpoint
    let getCount (txHash: byte[]) =
        let (f, count) = counts.TryGetValue(txHash)
        if f 
        then count + baseUTXO.GetCount(txHash)
        else baseUTXO.GetCount(txHash)

    let commit() = 
        table |> Seq.iter(fun kv ->
            let outpoint = kv.Key
            let utxo = kv.Value
            if utxo <> null 
            then baseUTXO.AddUTXO(outpoint, utxo)
            else baseUTXO.DeleteUTXO(outpoint)
        )
        table.Clear()
        counts.Clear()

    member x.Clear() = table.Clear()
    member x.Commit() = commit()

    interface IUTXOAccessor with
        member x.DeleteUTXO(outpoint) = deleteUTXO outpoint
        member x.AddUTXO(outpoint, txOut) = addUTXO outpoint txOut
        member x.GetUTXO(outpoint) = getUTXO outpoint
        member x.GetCount(txHash) = getCount txHash
        member x.Dispose() = table.Clear()

let txHash = hashFromHex "d4c7e1458bc7d7c54e90cc95117afd95a7498931cc2aa11e18ab0c52fc4cc512"
let checkScript (utxoAccessor: IUTXOAccessor) (tx: Tx): Option<unit> = 
    let x = 
        tx.TxIns 
            |> Seq.mapi (fun i txIn ->
                let scriptEval = new Script.ScriptRuntime(Script.computeTxHash tx i)
                let outpoint = txIn.PrevOutPoint
                let utxo = utxoAccessor.GetUTXO outpoint
                utxo |> Option.map (fun utxo -> scriptEval.Verify(txIn.Script, utxo.TxOut.Script))
                ) 
            |> Seq.toList |> Option.sequence 
            |> Option.map(fun x -> x.All(fun x -> x)) // tx succeeds if all scripts succeed
    (x.IsSome && x.Value) |> errorIfFalse "script failure"

let mempoolAccessor = new MempoolUTXOAccessor(utxoAccessor)
let nopWriter = new NopUndoWriter()

let validate (tx: Tx) =
    maybe {
        do! checkScript mempoolAccessor tx
        processUTXO mempoolAccessor nopWriter false mempoolHeight tx |> ignore
        return ()
    }

let revalidate () =
    mempoolAccessor.Clear()
    let newListTx = new List<byte[]>()
    let newMempool = new Dictionary<byte[], Tx>(new HashCompare())
    listTx <- listTx.Where(fun hash ->
        let tx = mempool.[hash]
        (validate tx).IsSome
        ).ToList()
    mempool <- listTx.Select(fun hash -> mempool.[hash]).ToDictionary(fun tx -> tx.Hash)
    logger.InfoF "Mempool has %d transactions" mempool.Count

let addTx tx = 
    try
        validate tx |> Option.iter(fun () ->
            listTx.Add(tx.Hash)
            mempool.Item(tx.Hash) <- tx
            broadcastToPeers.OnNext(new BitcoinMessage("tx", tx.ToByteArray()))
            logger.DebugF "Unconfirmed TX -> %s" (hashToHex tx.Hash)
            )
    with
    | ValidationException e -> ignore() // printfn "Invalid tx: %s" (hashToHex tx.Hash)

let processCommand (command: MempoolCommand) = 
    match command with
    | Revalidate (currentHeight, undoTxs) ->
        mempoolHeight <- currentHeight
        for txBlock in undoTxs do
            for tx in txBlock do
                listTx.Add(tx.Hash)
                mempool.Item(tx.Hash) <- tx
        revalidate()
    | Tx tx -> addTx tx
    | Inv (invVector, peer) -> 
        let newInvs = invVector.Invs |> List.filter(fun inv -> not (mempool.ContainsKey inv.Hash))
        newInvs |> List.iter (fun inv -> mempool.Add(inv.Hash, null))
        let gd = new GetData(newInvs)
        if not gd.Invs.IsEmpty then
            peer.Receive(GetData gd)
    | GetTx (invs, peer) ->
        for inv in invs do
            let (f, tx) = mempool.TryGetValue(inv.Hash)
            if f && tx <> null then
                peer.Send(new BitcoinMessage("tx", tx.ToByteArray()))
    | Mempool peer -> 
        let inv = mempool.Keys |> Seq.map (fun txHash -> InvEntry(txInvType, txHash)) |> Seq.toList
        logger.DebugF "MemoryPool %A" inv
        if not inv.IsEmpty then
            let invVec = InvVector(inv)
            peer.Send(new BitcoinMessage("inv", invVec.ToByteArray()))

let startMempool() =
    mempoolIncoming.ObserveOn(NewThreadScheduler.Default).Subscribe(processCommand) |> ignore
