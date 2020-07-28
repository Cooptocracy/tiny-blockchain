module Script
open System
open System.Collections.Generic
open System.Linq
open System.IO
open MoreLinq
open FSharpx
open FSharpx.Option
open Org.BouncyCastle.Utilities.Encoders
open Org.BouncyCastle.Crypto.Digests
open Org.BouncyCastle.Crypto.Parameters
open Org.BouncyCastle.Asn1
open Secp256k1
open Org.BouncyCastle.Crypto.Signers
open Protocol

let OP_DUP = 118uy
let OP_1 = 81uy 
let OP_16 = 96uy
let OP_HASH160 = 169uy
let OP_DATA_20 = 20uy
let OP_DATA_32 = 32uy
let OP_EQUAL = 135uy
let OP_EQUALVERIFY = 136uy 
[<Literal>] 
let OP_CHECKSIG = 172uy
[<Literal>] 
let OP_CHECKSIGVERIFY = 173uy 
[<Literal>] 
let OP_CHECKMULTISIG = 174uy
[<Literal>] 
let OP_CHECKMULTISIGVERIFY = 175uy
let OP_DATA_65 = 65uy
let OP_DATA_33 = 33uy 

let stackMaxDepth = 1000
let maxPushLength = 520
let maxMultiSig = 20
let maxOpCodeCount = 201
let maxSigOpCount = 201
let maxScriptSize = 10000

let scriptToHash (script: byte[]) =
    let p2pkh = if (script.Length = 25 && script.[0] = OP_DUP && script.[1] = OP_HASH160 && script.[2] = OP_DATA_20 && script.[23] = OP_EQUALVERIFY && script.[24] = OP_CHECKSIG) then Some script.[3..22] else None
    let p2sh = if (script.Length = 23 && script.[0] = OP_HASH160 && script.[1] = OP_DATA_20 && script.[22] = OP_EQUAL) then Some script.[2..21] else None
    Option.coalesce p2pkh p2sh

let isP2SH (script: byte[]) = script.Length = 23 && script.[0] = OP_HASH160 && script.[1] = OP_DATA_20 && script.[22] = OP_EQUAL

let isPay2PubKey (script: byte[]) = script.Length > 0 && script.[script.Length-1] = OP_CHECKSIG
let isPay2MultiSig (script: byte[]) = script.Length > 0 && script.[script.Length-1] = OP_CHECKMULTISIG

let ECDSACheck (txHash: byte[], pub: byte[], signature: byte[]) = 
    if signature.Length > 0 && pub.Length > 0 then // 0 length signature or pub keys crash the library
        let result = Signatures.Verify(txHash, signature, pub)
        result = Signatures.VerifyResult.Verified
    else false

type List<'a> with
    member x.Push(v: 'a) = 
        x.Add(v)
    member x.Pop() = 
        let v = x.Peek()
        x.RemoveAt(x.Count-1)
        v
    member x.Peek() = x.Item(x.Count-1)

type ByteList() =
    inherit List<byte[]>()
    member x.Push(v: byte[]) = 
        base.Add(v)

let computeTxHash (tx: Tx) (index: int) (subScript: byte[]) (sigType: int) = 
    let mutable returnBuggyHash = false

    let anyoneCanPay = (sigType &&& 0x80) <> 0
    let sigHashType = 
        match sigType &&& 0x1F with
        | 2 -> 2uy
        | 3 -> 3uy
        | _ -> 1uy
    use oms = new MemoryStream()
    use writer = new BinaryWriter(oms)
    writer.Write(tx.Version)

    if anyoneCanPay then 
        writer.WriteVarInt(1)
        let txIn2 = new TxIn(tx.TxIns.[index].PrevOutPoint, subScript, tx.TxIns.[index].Sequence)
        writer.Write(txIn2.ToByteArray())
    else 
        writer.WriteVarInt(tx.TxIns.Length)
        tx.TxIns |> Array.iteri (fun i txIn ->
            let script = if i = index then subScript else Array.empty
            let txIn2 = 
                if sigHashType <> 1uy && i <> index then 
                    new TxIn(txIn.PrevOutPoint, script, 0)
                else
                    new TxIn(txIn.PrevOutPoint, script, txIn.Sequence)
            writer.Write(txIn2.ToByteArray())
        )
    
    match sigHashType with
    | 1uy ->
        writer.WriteVarInt(tx.TxOuts.Length)
        for txOut in tx.TxOuts do
            writer.Write(txOut.ToByteArray())
    | 2uy ->
        writer.WriteVarInt 0
    | 3uy -> 
        if index >= tx.TxOuts.Length then
            returnBuggyHash <- true
        else
            writer.WriteVarInt (index+1)
            for i in 0..index do
                if i < index then
                    let txOut = new TxOut(-1L, Array.empty)
                    writer.Write(txOut.ToByteArray())
                else
                    writer.Write(tx.TxOuts.[i].ToByteArray())
    | _ -> ignore()
    
    writer.Write(tx.LockTime)
    writer.Write(sigType)

    if returnBuggyHash then
        let hash = Array.zeroCreate<byte> 32
        hash.[0] <- 1uy
        hash
    else
        let data = oms.ToArray()
        dsha data

let bigintToBytes (i: bigint) =
    let pos = abs i
    let bi = pos.ToByteArray()
    let posTrimIdx = revFind bi (fun b -> b <> 0uy)
    let iTrimIdx = 
        if (posTrimIdx >= 0 && (bi.[posTrimIdx] &&& 0x80uy) <> 0uy)
            then posTrimIdx + 1
            else posTrimIdx
    let bytes = bi.[0..iTrimIdx]
    if i < 0I then
        bytes.[iTrimIdx] <- bytes.[iTrimIdx] ||| 0x80uy
    bytes

let intToBytes (i: int) = bigintToBytes (bigint i)
let int64ToBytes (i: int64) = bigintToBytes (bigint i)
        
let bytesToInt (bytes: byte[]) =
    let b = Array.zeroCreate<byte> bytes.Length
    Array.Copy(bytes, b, bytes.Length)
    let neg = 
        if b.Length > 0 && b.[b.Length-1] &&& 0x80uy <> 0uy then
            b.[b.Length-1] <- b.[b.Length-1] &&& 0x7Fuy
            true
        else false

    let bi = bigint b
    if neg then -bi else bi

type ScriptRuntime(getTxHash: byte[] -> int -> byte[]) =
    let evalStack = new ByteList()
    let altStack = new ByteList()
    let ifStack = new List<bool>()
    let mutable skipping = false
    let mutable codeSep = 0

    let checkDepth (stack: List<'a>) minDepth = 
        if stack.Count < minDepth then raise (ValidationException "not enough stack depth")
    let checkMaxDepth () =
        if evalStack.Count + altStack.Count > stackMaxDepth then raise (ValidationException "stack too deep")
    let popAsBool() = 
        checkDepth evalStack 1
        bytesToInt(evalStack.Pop()) <> 0I
    let verify() =
        if not (popAsBool()) 
        then raise (ValidationException "verification failed")
    let fail() = 
        evalStack.Push(intToBytes(0))
        raise (ValidationException "verification failed")
    let checkOverflow (bytes: byte[]) =
        if bytes.Length > 4 then fail()
        bytes
    let checkIfStackEmpty() = 
        if ifStack.Count > 0 then fail()

    let roll m =
        let n = m+1
        checkDepth evalStack n
        let i = evalStack.Count-n
        let top = evalStack.Item i
        evalStack.RemoveAt i
        evalStack.Push top

    let dup n depth =
        checkDepth evalStack (n+depth)
        for i in 1..n do
            evalStack.Push(evalStack.Item(evalStack.Count-n-depth))
        checkMaxDepth()

    let unaryOp (f: int64 -> int64) =
        checkDepth evalStack 1
        let arg = evalStack.Pop() |> checkOverflow |> bytesToInt |> int64
        f(arg) |> int64ToBytes |> evalStack.Push

    let binaryOp (f: int64 -> int64 -> int64) = 
        checkDepth evalStack 2
        let arg2 = evalStack.Pop() |> checkOverflow |> bytesToInt |> int64
        let arg1 = evalStack.Pop() |> checkOverflow |> bytesToInt |> int64
        f arg1 arg2 |> int64ToBytes |> evalStack.Push

    let logicalOp (f: bool -> bool -> bool) = 
        checkDepth evalStack 2
        let arg2 = evalStack.Pop() |> checkOverflow |> bytesToInt |> int |> fun x -> x <> 0
        let arg1 = evalStack.Pop() |> checkOverflow |> bytesToInt |> int |> fun x -> x <> 0
        f arg1 arg2 |> (fun x -> if x then 1 else 0) |> intToBytes |> evalStack.Push

    let binaryBoolOp (f: int -> int -> bool) = 
        checkDepth evalStack 2
        let arg2 = evalStack.Pop() |> checkOverflow |> bytesToInt |> int
        let arg1 = evalStack.Pop() |> checkOverflow |> bytesToInt |> int
        f arg1 arg2 |> (fun x -> if x then 1 else 0) |> intToBytes |> evalStack.Push

    let tertiaryOp (f: int -> int -> int -> int) = 
        checkDepth evalStack 3
        let arg3 = evalStack.Pop() |> checkOverflow |> bytesToInt |> int
        let arg2 = evalStack.Pop() |> checkOverflow |> bytesToInt |> int
        let arg1 = evalStack.Pop() |> checkOverflow |> bytesToInt |> int
        f arg1 arg2 arg3 |> intToBytes |> evalStack.Push

    let hashOp (f: byte[] -> byte[]) =
        checkDepth evalStack 1
        let data = evalStack.Pop()
        data |> f |> evalStack.Push

    let removeData (script: byte[]) (pred: byte[] -> bool) = 
        let dataList = new List<byte[]>()
        use ms = new MemoryStream(script)
        use reader = new BinaryReader(ms)
        (
            try
                use oms = new MemoryStream()
                use writer = new BinaryWriter(oms)

                let rec removeDataInner (): byte[] =
                    if ms.Position < ms.Length then
                        let b = reader.ReadByte()
                        let c = int b
                        if c >= 1 && c <= 78 then
                            let startPos = ms.Position-1L
                            let len = 
                                match c with
                                | 76 -> int (reader.ReadByte())
                                | 77 -> int (reader.ReadInt16())
                                | 78 -> reader.ReadInt32()
                                | x -> x
                            let canonical = 
                                if len < 75 then len
                                elif len < 0xFF then 76
                                elif len < 0xFFFF then 77
                                else 78

                            let data = reader.ReadBytes(len)
                            if canonical <> c || not (pred(data)) then
                                let lenRead = int(ms.Position - startPos)
                                ms.Seek(startPos, SeekOrigin.Begin) |> ignore
                                writer.Write(reader.ReadBytes(lenRead))
                            dataList.Add(data)
                        elif b <> 171uy then
                            writer.Write(b)
                        removeDataInner()
                    else
                        oms.ToArray()

                let script = removeDataInner()
                ms.Dispose()
                (true, script, dataList.ToArray())
            with 
            | e -> 
                logger.DebugF "Invalid script %A" e
                let currentPos = int ms.Position-1
                (false, script.[0..currentPos], dataList.ToArray())
        )

    let removeSignatures (script: byte[]) (signatures: HashSet<byte[]>) = 
        let subScript = 
            if codeSep <> 0
                then script.[codeSep..]
                else script
        let (success, script, _) = removeData subScript (fun data -> signatures.Contains data)
        if not success then fail() 
        script

    let getData (script: byte[]) = 
        let (_, _, data) = removeData script (fun _ -> true)
        data

    let scriptCheckSigCount (script: byte[]) =
        let (_, ops, _) = removeData script (fun _ -> true)
        ops.Prepend(0xFFuy).Window(2) |> Seq.map (fun x -> // Prepend a dummy byte to deal with boundary case
            match x.ToArray() with
            | [|op1; op2|] ->
                match op2 with
                | OP_CHECKSIG | OP_CHECKSIGVERIFY -> 1 // single signature check counts as 1
                | OP_CHECKMULTISIG | OP_CHECKMULTISIGVERIFY -> // if preceeded by OP_X, multi-sig counts as x, 
                    if op1 >= OP_1 && op1 <= OP_16 // otherwise counts as 20
                    then int (op1-OP_1)
                    else 20
                | _ -> 0
            | _ -> 0
            ) |> Seq.sum

    let checksigInner pubScript pub (signature: byte[]) = 
        let sigType = if signature.Length > 0 then signature.[signature.Length-1] else 0uy
        let txHash = getTxHash pubScript (int sigType)
        ECDSACheck(txHash, pub, signature)

    let checksig script pub signature = 
        let pubScript = removeSignatures script (new HashSet<byte[]>([signature], hashCompare))
        checksigInner pubScript pub signature

    let checkmultisig script (pubs: byte[][]) (sigs: byte[] list) = 
        let pubScript = removeSignatures script (new HashSet<byte[]>(sigs, hashCompare))

        let checkOneSig (pubs: byte[] list) (signature: byte[]) = 
            pubs |> List.tryFindIndex (fun pub -> checksigInner pubScript pub signature)
            |> Option.map (fun index -> pubs |> Seq.skip (index+1) |> Seq.toList)
                
        sigs |> Option.foldM checkOneSig (pubs |> Array.toList) |> Option.isSome
    
    let eval (pushOnly: bool) (script: byte[]) =
        codeSep <- 0
        if script.Length > maxScriptSize then fail()
        let ms = new MemoryStream(script)
        let reader = new BinaryReader(ms)

        let rec innerEval (opCount: int) =
            let mutable multiSigOps = 0
            if opCount > maxOpCodeCount then fail()
            if ms.Position < ms.Length then
                let c = int (reader.ReadByte())
                // logger.DebugF "Stack size %d op = %x opc = %d" evalStack.Count c opCount
                if c = 0 then
                    if not skipping then
                        evalStack.Push(Array.empty)
                elif c = 79 then
                    if not skipping then
                        evalStack.Push([| 0x81uy |])

                elif c >= 1 && c <= 78 then
                    let len = 
                        match c with
                        | 76 -> int (reader.ReadByte())
                        | 77 -> int (reader.ReadInt16())
                        | 78 -> reader.ReadInt32()
                        | x -> x
                    let data = reader.ReadBytes(len)
                    if data.Length < len then raise (ValidationException "Not enough data to read")
                    if len > maxPushLength then raise (ValidationException "PushData exceeds 520 bytes")
                    if not skipping then
                        evalStack.Push(data)

                elif c >= 81 && c <= 96 then
                    if not skipping then
                        evalStack.Push(intToBytes (c-80))
                elif pushOnly then fail()

                elif c = 99 || c = 100 then
                    ifStack.Push(skipping)
                    if not skipping then
                        skipping <- not (popAsBool())
                        if c = 100 then skipping <- not skipping
                elif c = 103 then
                    checkDepth ifStack 1
                    if not (ifStack.Peek()) then skipping <- not skipping
                elif c = 104 then
                    checkDepth ifStack 1
                    skipping <- ifStack.Pop()
                elif c >= 176 && c <= 185 then ignore()
                elif 
                    match c with
                    | 101 | 102 
                    | 126 | 127 | 128 | 129 | 131 | 132 | 133 | 134
                    | 141 | 142 | 149 | 150 | 151 | 152 | 153 -> true
                    | _ -> false
                    then fail()
                elif not skipping then
                    match c with
                    | 97 -> ignore()
                    | 105 -> verify()
                    | 106 | 80 | 98 | 137 | 138 -> fail()
                    | 107 -> 
                        checkDepth evalStack 1
                        let top = evalStack.Pop()
                        altStack.Push(top)
                    | 108 ->
                        checkDepth altStack 1
                        let top = altStack.Pop()
                        evalStack.Push(top)
                    | 109 ->
                        checkDepth evalStack 2
                        evalStack.Pop() |> ignore
                        evalStack.Pop() |> ignore
                    | 110 -> dup 2 0
                    | 111 -> dup 3 0
                    | 112 -> dup 2 2
                    | 113 -> 
                        roll 5
                        roll 5
                    | 114 ->
                        roll 3
                        roll 3
                    | 115 ->
                        checkDepth evalStack 1
                        let t = evalStack.Peek()
                        evalStack.Push t
                        if popAsBool() then evalStack.Push t
                    | 116 -> evalStack.Push(intToBytes (evalStack.Count))
                    | 117 ->
                        checkDepth evalStack 1
                        evalStack.Pop() |> ignore
                    | 118 -> 
                        checkDepth evalStack 1
                        evalStack.Push(evalStack.Peek())
                    | 119 -> 
                        checkDepth evalStack 2
                        evalStack.RemoveAt(evalStack.Count-2)
                    | 120 ->
                        checkDepth evalStack 2
                        evalStack.Push(evalStack.Item(evalStack.Count-2))
                    | 121 ->
                        let n = int(bytesToInt(evalStack.Pop()))
                        if n < 0 then fail()
                        checkDepth evalStack (n+1)
                        evalStack.Push(evalStack.Item(evalStack.Count-1-n))
                    | 122 ->
                        let n = int(bytesToInt(evalStack.Pop()))
                        if n < 0 then fail()
                        roll n
                    | 123 -> roll 2
                    | 124 ->
                        checkDepth evalStack 2
                        let a = evalStack.Pop()
                        let b = evalStack.Pop()
                        evalStack.Push a
                        evalStack.Push b
                    | 125 ->
                        checkDepth evalStack 2
                        let top = evalStack.Peek()
                        evalStack.Insert(evalStack.Count-2, top)
                    | 130 -> 
                        checkDepth evalStack 1
                        let top = evalStack.Peek()
                        evalStack.Push(intToBytes top.Length)
                    | 135 | 136 ->
                        checkDepth evalStack 2
                        let a = evalStack.Pop()
                        let b = evalStack.Pop()
                        let eq = if hashCompare.Equals(a, b) then 1 else 0
                        evalStack.Push(intToBytes eq)
                        if c = 136 then verify()
                    | 139 ->
                        unaryOp(fun x -> x+1L)
                    | 140 ->
                        unaryOp(fun x -> x-1L)
                    | 143 ->
                        unaryOp(fun x -> -x)
                    | 144 ->
                        unaryOp(fun x -> if x > 0L then x else -x)
                    | 145 ->
                        unaryOp(fun x -> if x <> 0L then 0L else 1L)
                    | 146 ->
                        unaryOp(fun x -> if x <> 0L then 1L else 0L)
                    | 147 ->
                        binaryOp(fun x y -> x+y)
                    | 148 ->
                        binaryOp(fun x y -> x-y)
                    | 154 ->
                        logicalOp(fun x y -> x&&y)
                    | 155 ->
                        logicalOp(fun x y -> x||y)
                    | 156 ->
                        logicalOp(fun x y -> x=y)
                    | 157 ->
                        logicalOp(fun x y -> x=y)
                        verify()
                    | 158 ->
                        binaryBoolOp(fun x y -> x<>y)
                    | 159 ->
                        binaryBoolOp(fun x y -> x<y)
                    | 160 ->
                        binaryBoolOp(fun x y -> x>y)
                    | 161 ->
                        binaryBoolOp(fun x y -> x<=y)
                    | 162 ->
                        binaryBoolOp(fun x y -> x>=y)
                    | 163 ->
                        binaryOp(fun x y -> if x<y then x else y)
                    | 164 ->
                        binaryOp(fun x y -> if x>y then x else y)
                    | 165 ->
                        tertiaryOp(fun x a b -> if x>=a && x<b then 1 else 0)
                    | 166 ->
                        hashOp(ripe160)
                    | 167 ->
                        hashOp(sha1)
                    | 168 ->
                        hashOp(sha256)
                    | 169 ->
                        hashOp(hash160)
                    | 170 ->
                        hashOp(dsha)
                    | 171 ->
                        codeSep <- int(reader.BaseStream.Position)
                    | 172 | 173 ->
                        checkDepth evalStack 2
                        let pub = evalStack.Pop()
                        let signature = evalStack.Pop()
                        let check = checksig script pub signature
                        evalStack.Push(intToBytes(if check then 1 else 0))
                        if c = 173 then verify()
                    | 174 | 175 ->
                        checkDepth evalStack 1
                        let nPubs = int(bytesToInt(evalStack.Pop()))
                        multiSigOps <- nPubs
                        if nPubs > maxMultiSig then fail()
                        checkDepth evalStack nPubs
                        let pubs = seq { for _ in 1..nPubs do yield evalStack.Pop() } |> Seq.toArray
                        let nSigs = int(bytesToInt(evalStack.Pop()))
                        if nSigs > nPubs then fail()
                        checkDepth evalStack nSigs
                        let sigs = seq { for _ in 1..nSigs do yield evalStack.Pop() } |> Seq.toList
                        checkDepth evalStack 1
                        let dummy = evalStack.Pop()
                        if dummy.Length > 0 then fail()
                        let check = checkmultisig script pubs sigs
                        evalStack.Push(intToBytes(if check then 1 else 0))
                        if c = 175 then verify()
                    | _ -> fail()

                innerEval ((if c < 97 then opCount else (opCount+1))+multiSigOps)

        innerEval 0
        checkIfStackEmpty()
    let evalP2SH (script: byte[]) =
        evalStack.Clear()
        eval true script
        checkDepth evalStack 1
        let redeemScript = evalStack.Pop()
        eval false redeemScript
        popAsBool()

    let redeemScript (script: byte[]) =
        let (_, _, data) = removeData script (fun _ -> true)
        data |> Array.last

    member x.Verify(inScript: byte[], outScript: byte[]) = 
        try
            eval false inScript
            altStack.Clear() // bitcoin core does that
            ifStack.Clear() // 
            eval false outScript
            let res = popAsBool()
            res && (not (isP2SH outScript) || evalP2SH inScript)
        with
        | :? ValidationException -> false

    member x.CheckSigCount (script: byte[]) = scriptCheckSigCount script
    member x.RedeemScript (script: byte[]) = redeemScript script
    static member IsP2SH (script: byte[]) = isP2SH script

    member x.GetData (script: byte[]) = getData script
